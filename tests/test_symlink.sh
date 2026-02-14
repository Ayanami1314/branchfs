#!/bin/bash
# Test symlink operations (create, readlink, COW, commit, abort)

source "$(dirname "$0")/test_helper.sh"

test_base_symlink_visible() {
    setup

    # Add a symlink to the base directory
    ln -s file1.txt "$TEST_BASE/link_to_file1"
    ln -s subdir "$TEST_BASE/link_to_subdir"

    do_mount

    # Symlink should be visible and identified as a symlink
    assert "[[ -L '$TEST_MNT/link_to_file1' ]]" "Base symlink is visible as symlink"
    assert "[[ -L '$TEST_MNT/link_to_subdir' ]]" "Base dir symlink is visible as symlink"

    # readlink should return the target
    local target
    target=$(readlink "$TEST_MNT/link_to_file1")
    assert_eq "$target" "file1.txt" "readlink returns correct target"

    target=$(readlink "$TEST_MNT/link_to_subdir")
    assert_eq "$target" "subdir" "readlink on dir symlink returns correct target"

    # Following the symlink should work
    local content
    content=$(cat "$TEST_MNT/link_to_file1")
    assert_eq "$content" "base content" "Reading through symlink works"

    do_unmount
}

test_base_dangling_symlink_visible() {
    setup

    # Create a dangling symlink in base
    ln -s nonexistent_target "$TEST_BASE/dangling_link"

    do_mount

    # Dangling symlink should be visible as a symlink
    assert "[[ -L '$TEST_MNT/dangling_link' ]]" "Dangling symlink is visible"

    # readlink should return the target
    local target
    target=$(readlink "$TEST_MNT/dangling_link")
    assert_eq "$target" "nonexistent_target" "readlink on dangling symlink works"

    # But following it should fail
    assert "[[ ! -e '$TEST_MNT/dangling_link' ]]" "Dangling symlink target does not exist"

    do_unmount
}

test_create_symlink_in_branch() {
    setup
    do_mount
    do_create "symlink_create" "main"

    # Create a symlink in the branch
    ln -s file1.txt "$TEST_MNT/new_link"

    assert "[[ -L '$TEST_MNT/new_link' ]]" "New symlink exists"

    local target
    target=$(readlink "$TEST_MNT/new_link")
    assert_eq "$target" "file1.txt" "New symlink has correct target"

    # Following the symlink should work
    local content
    content=$(cat "$TEST_MNT/new_link")
    assert_eq "$content" "base content" "Reading through new symlink works"

    # Symlink should NOT exist in base
    assert "[[ ! -L '$TEST_BASE/new_link' ]]" "Symlink not in base yet"

    do_unmount
}

test_symlink_cow_preserves_link() {
    setup

    # Add a symlink to base
    ln -s file1.txt "$TEST_BASE/base_link"

    do_mount
    do_create "symlink_cow" "main"

    # The symlink should still be a symlink (not a regular file)
    assert "[[ -L '$TEST_MNT/base_link' ]]" "Base symlink still visible in branch"

    local target
    target=$(readlink "$TEST_MNT/base_link")
    assert_eq "$target" "file1.txt" "Symlink target preserved in branch"

    # Base should be unchanged
    assert "[[ -L '$TEST_BASE/base_link' ]]" "Base symlink unchanged"

    do_unmount
}

test_symlink_readdir_type() {
    setup

    # Add symlinks to base
    ln -s file1.txt "$TEST_BASE/link1"
    ln -s nonexistent "$TEST_BASE/dangling"

    do_mount

    # ls -la should show 'l' for symlinks
    local ls_output
    ls_output=$(ls -la "$TEST_MNT/" | grep "link1")
    assert "[[ '$ls_output' == l* ]]" "ls shows symlink type for link1"

    ls_output=$(ls -la "$TEST_MNT/" | grep "dangling")
    assert "[[ '$ls_output' == l* ]]" "ls shows symlink type for dangling link"

    do_unmount
}

test_symlink_commit_to_base() {
    setup
    do_mount
    do_create "symlink_commit" "main"

    # Create a symlink in the branch
    ln -s file2.txt "$TEST_MNT/committed_link"
    assert "[[ -L '$TEST_MNT/committed_link' ]]" "Symlink exists before commit"

    # Commit
    do_commit

    # Symlink should now exist in base as a symlink
    assert "[[ -L '$TEST_BASE/committed_link' ]]" "Symlink in base after commit"

    local target
    target=$(readlink "$TEST_BASE/committed_link")
    assert_eq "$target" "file2.txt" "Committed symlink has correct target"

    do_unmount
}

test_symlink_abort_discards() {
    setup
    do_mount
    do_create "symlink_abort" "main"

    # Create a symlink in the branch
    ln -s file1.txt "$TEST_MNT/aborted_link"
    assert "[[ -L '$TEST_MNT/aborted_link' ]]" "Symlink exists before abort"

    # Abort
    do_abort

    # Symlink should NOT exist in base
    assert "[[ ! -L '$TEST_BASE/aborted_link' ]]" "Symlink not in base after abort"
    assert "[[ ! -e '$TEST_BASE/aborted_link' ]]" "No entry in base after abort"

    do_unmount
}

test_symlink_delete_in_branch() {
    setup

    ln -s file1.txt "$TEST_BASE/link_to_delete"

    do_mount
    do_create "symlink_delete" "main"

    assert "[[ -L '$TEST_MNT/link_to_delete' ]]" "Symlink exists before delete"

    # Delete the symlink
    rm "$TEST_MNT/link_to_delete"
    assert "[[ ! -L '$TEST_MNT/link_to_delete' ]]" "Symlink deleted in branch"

    # Base should still have it
    assert "[[ -L '$TEST_BASE/link_to_delete' ]]" "Base symlink still exists"

    do_unmount
}

test_symlink_in_branch_dir() {
    setup

    # Add a symlink to base (may already exist from prior test reusing dirs)
    ln -sf file1.txt "$TEST_BASE/base_link"

    do_mount
    do_create "symlink_branch_dir" "main"

    # Symlinks should be visible through @branch dir
    assert "[[ -L '$TEST_MNT/@symlink_branch_dir/base_link' ]]" "Symlink visible in @branch dir"

    local target
    target=$(readlink "$TEST_MNT/@symlink_branch_dir/base_link")
    assert_eq "$target" "file1.txt" "readlink works in @branch dir"

    # Create a new symlink through @branch dir
    ln -s file2.txt "$TEST_MNT/@symlink_branch_dir/branch_link"
    assert "[[ -L '$TEST_MNT/@symlink_branch_dir/branch_link' ]]" "New symlink in @branch dir"

    local target2
    target2=$(readlink "$TEST_MNT/@symlink_branch_dir/branch_link")
    assert_eq "$target2" "file2.txt" "readlink on new symlink in @branch dir"

    do_unmount
}

# Run tests
run_test "Base Symlink Visible" test_base_symlink_visible
run_test "Dangling Symlink Visible" test_base_dangling_symlink_visible
run_test "Create Symlink in Branch" test_create_symlink_in_branch
run_test "Symlink COW Preserves Link" test_symlink_cow_preserves_link
run_test "Symlink Readdir Type" test_symlink_readdir_type
run_test "Symlink Commit to Base" test_symlink_commit_to_base
run_test "Symlink Abort Discards" test_symlink_abort_discards
run_test "Symlink Delete in Branch" test_symlink_delete_in_branch
run_test "Symlink in Branch Dir" test_symlink_in_branch_dir

print_summary
