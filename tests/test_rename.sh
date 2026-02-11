#!/bin/bash
# Test rename/mv operations

source "$(dirname "$0")/test_helper.sh"

test_rename_same_dir() {
    setup
    do_mount
    do_create "rename_same" "main"

    mv "$TEST_MNT/file1.txt" "$TEST_MNT/file1_renamed.txt"
    assert_file_not_exists "$TEST_MNT/file1.txt" "Old name gone after rename"
    assert_file_exists "$TEST_MNT/file1_renamed.txt" "New name exists after rename"
    assert_file_contains "$TEST_MNT/file1_renamed.txt" "base content" "Renamed file has correct content"

    # Base unchanged
    assert_file_exists "$TEST_BASE/file1.txt" "Base file untouched"

    do_unmount
}

test_rename_cross_dir() {
    setup
    do_mount
    do_create "rename_xdir" "main"

    mv "$TEST_MNT/file1.txt" "$TEST_MNT/subdir/moved.txt"
    assert_file_not_exists "$TEST_MNT/file1.txt" "Old location empty"
    assert_file_exists "$TEST_MNT/subdir/moved.txt" "File moved to subdir"
    assert_file_contains "$TEST_MNT/subdir/moved.txt" "base content" "Moved file has correct content"

    do_unmount
}

test_rename_overwrite() {
    setup
    do_mount
    do_create "rename_over" "main"

    echo "new content" > "$TEST_MNT/new.txt"
    mv "$TEST_MNT/new.txt" "$TEST_MNT/file2.txt"
    assert_file_not_exists "$TEST_MNT/new.txt" "Source gone after overwrite rename"
    assert_file_contains "$TEST_MNT/file2.txt" "new content" "Overwritten file has new content"

    do_unmount
}

test_rename_new_file() {
    setup
    do_mount
    do_create "rename_new" "main"

    echo "created" > "$TEST_MNT/original.txt"
    mv "$TEST_MNT/original.txt" "$TEST_MNT/moved.txt"
    assert_file_not_exists "$TEST_MNT/original.txt" "Original gone"
    assert_file_exists "$TEST_MNT/moved.txt" "Moved file exists"
    assert_file_contains "$TEST_MNT/moved.txt" "created" "Moved file content correct"

    do_unmount
}

test_rename_commit() {
    setup
    do_mount
    do_create "rename_commit" "main"

    mv "$TEST_MNT/file1.txt" "$TEST_MNT/renamed.txt"
    do_commit

    assert_file_exists "$TEST_BASE/renamed.txt" "Renamed file in base after commit"
    assert_file_contains "$TEST_BASE/renamed.txt" "base content" "Committed renamed file content correct"
    assert_file_not_exists "$TEST_BASE/file1.txt" "Original deleted from base after commit"

    do_unmount
}

test_rename_abort() {
    setup
    do_mount
    do_create "rename_abort" "main"

    mv "$TEST_MNT/file1.txt" "$TEST_MNT/renamed.txt"
    do_abort

    assert_file_exists "$TEST_BASE/file1.txt" "Base file still exists after abort"
    assert_file_not_exists "$TEST_BASE/renamed.txt" "Renamed file not in base after abort"

    do_unmount
}

test_rename_nonexistent() {
    setup
    do_mount
    do_create "rename_noent" "main"

    if mv "$TEST_MNT/no_such_file.txt" "$TEST_MNT/dest.txt" 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} Rename nonexistent should fail"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} Rename nonexistent correctly fails"
    fi

    do_unmount
}

# Run tests (abort before commit to avoid leftover base state between tests)
run_test "Rename Same Dir" test_rename_same_dir
run_test "Rename Cross Dir" test_rename_cross_dir
run_test "Rename Overwrite Existing" test_rename_overwrite
run_test "Rename New File" test_rename_new_file
run_test "Rename Nonexistent Fails" test_rename_nonexistent
run_test "Rename + Abort" test_rename_abort
run_test "Rename + Commit" test_rename_commit

print_summary
