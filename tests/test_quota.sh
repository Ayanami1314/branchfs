#!/bin/bash
# Test storage quota enforcement (--max-storage)

source "$(dirname "$0")/test_helper.sh"

# Mount with a storage quota
do_mount_with_quota() {
    local quota="$1"
    local extra_args=()
    if [[ "$(id -u)" == "0" ]]; then
        extra_args+=(--passthrough)
    fi
    "$BRANCHFS" mount --base "$TEST_BASE" --storage "$TEST_STORAGE" \
        --max-storage "$quota" "${extra_args[@]}" "$TEST_MNT"
    sleep 0.5
}

test_write_within_quota() {
    setup
    do_mount_with_quota "1M"
    do_create "quota_ok" "main"

    # Write a small file — should succeed
    echo "hello" > "$TEST_MNT/small.txt"
    assert_file_contains "$TEST_MNT/small.txt" "hello" "Small write within quota succeeds"

    do_unmount
}

test_write_exceeds_quota() {
    setup
    do_mount_with_quota "10K"
    do_create "quota_exceed" "main"

    # Write a file larger than the quota — should fail with ENOSPC
    if dd if=/dev/zero of="$TEST_MNT/big.txt" bs=1K count=20 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} Large write should have failed with ENOSPC"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} Large write rejected (ENOSPC)"
    fi

    do_unmount
}

test_cow_exceeds_quota() {
    setup

    # Create a base file larger than the quota
    dd if=/dev/urandom of="$TEST_BASE/large_base.bin" bs=1K count=50 2>/dev/null

    do_mount_with_quota "10K"
    do_create "cow_exceed" "main"

    # Try to modify the large base file — COW copy should fail
    if echo "modified" > "$TEST_MNT/large_base.bin" 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} COW of large file should have failed with ENOSPC"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} COW copy rejected when exceeds quota"
    fi

    # Original file should still be readable
    assert "[[ -f '$TEST_MNT/large_base.bin' ]]" "Original file still readable"

    do_unmount
}

test_delete_frees_quota() {
    setup
    do_mount_with_quota "20K"
    do_create "quota_free" "main"

    # Write a file that uses most of the quota
    dd if=/dev/zero of="$TEST_MNT/fill.txt" bs=1K count=15 2>/dev/null
    assert_file_exists "$TEST_MNT/fill.txt" "File created using most of quota"

    # Another write should fail
    if dd if=/dev/zero of="$TEST_MNT/overflow.txt" bs=1K count=10 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} Second write should have failed"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} Quota full, second write rejected"
    fi

    # Delete the first file to free space
    rm "$TEST_MNT/fill.txt"

    # Now writing should succeed again
    dd if=/dev/zero of="$TEST_MNT/after_delete.txt" bs=1K count=10 2>/dev/null
    assert_file_exists "$TEST_MNT/after_delete.txt" "Write succeeds after deleting file"

    do_unmount
}

test_statfs_reports_quota() {
    setup
    do_mount_with_quota "1M"

    # df should report the quota as total size
    local total_k
    total_k=$(df -k "$TEST_MNT" | tail -1 | awk '{print $2}')
    # 1M = 1048576 bytes = 256 blocks of 4096 = 1024K
    assert "[[ $total_k -le 1100 && $total_k -ge 900 ]]" \
        "statfs reports ~1M total (got ${total_k}K)"

    do_unmount
}

test_no_quota_unlimited() {
    setup
    do_mount  # No --max-storage

    # Should be able to write freely
    dd if=/dev/zero of="$TEST_MNT/free.txt" bs=1K count=100 2>/dev/null
    assert_file_exists "$TEST_MNT/free.txt" "Write without quota succeeds"

    # df should report underlying filesystem size (much larger than 1M)
    local total_k
    total_k=$(df -k "$TEST_MNT" | tail -1 | awk '{print $2}')
    assert "[[ $total_k -gt 10000 ]]" \
        "statfs reports large total without quota (got ${total_k}K)"

    do_unmount
}

# Run tests
run_test "Write Within Quota" test_write_within_quota
run_test "Write Exceeds Quota" test_write_exceeds_quota
run_test "COW Exceeds Quota" test_cow_exceeds_quota
run_test "Delete Frees Quota" test_delete_frees_quota
run_test "Statfs Reports Quota" test_statfs_reports_quota
run_test "No Quota Unlimited" test_no_quota_unlimited

print_summary
