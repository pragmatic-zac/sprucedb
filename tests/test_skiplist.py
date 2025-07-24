from src.skiplist import SkipList

def test_basic_insert_and_search() -> None:
    skiplist = SkipList[int]()
    
    skiplist.insert(5, 100)
    skiplist.insert(3, 200)
    skiplist.insert(7, 300)
    
    assert skiplist.search(5) == 100
    assert skiplist.search(3) == 200
    assert skiplist.search(7) == 300
    assert skiplist.search(4) is None  # Non-existent key

def test_empty_list() -> None:
    skiplist = SkipList[str]()
    assert skiplist.search(1) is None

def test_duplicate_keys() -> None:
    skiplist = SkipList[str]()
    
    # Insert with same key, different values
    skiplist.insert(1, "first")
    skiplist.insert(1, "second")
    
    # Should return the most recently inserted value
    assert skiplist.search(1) == "second"

def test_multiple_levels() -> None:
    skiplist = SkipList[int](p=0.5, max_level=4)
    
    # Insert enough items to likely create multiple levels
    for i in range(10):
        skiplist.insert(i, i * 10)
    
    # Verify all items can be found
    for i in range(10):
        assert skiplist.search(i) == i * 10

def test_different_value_types() -> None:
    # Test with string values
    str_skiplist = SkipList[str]()
    str_skiplist.insert(1, "hello")
    assert str_skiplist.search(1) == "hello"
    
    # Test with list values
    list_skiplist = SkipList[list]()
    list_skiplist.insert(1, [1, 2, 3])
    assert list_skiplist.search(1) == [1, 2, 3]
    
    # Test with tuple values
    tuple_skiplist = SkipList[tuple]()
    tuple_skiplist.insert(1, (1, 2, 3))
    assert tuple_skiplist.search(1) == (1, 2, 3)

def test_negative_keys() -> None:
    skiplist = SkipList[int]()
    
    skiplist.insert(-5, 100)
    skiplist.insert(-3, 200)
    skiplist.insert(-7, 300)
    
    assert skiplist.search(-5) == 100
    assert skiplist.search(-3) == 200
    assert skiplist.search(-7) == 300
    assert skiplist.search(-4) is None 

def test_objects() -> None:
    skiplist = SkipList[str]()  # Changed to use string keys which are comparable

    expected = "bar"
    skiplist.insert("foo", expected)

    assert skiplist.search("foo") == expected

def test_basic_delete() -> None:
    skiplist = SkipList[int]()
    
    # Insert some values
    skiplist.insert(5, 100)
    skiplist.insert(3, 200)
    skiplist.insert(7, 300)

    # Delete a value
    skiplist.delete(3)
    
    # Verify the deleted value is gone
    assert skiplist.search(3) is None
    # Verify other values are still there
    assert skiplist.search(5) == 100
    assert skiplist.search(7) == 300

def test_delete_nonexistent_key() -> None:
    skiplist = SkipList[int]()
    
    # Insert a value
    skiplist.insert(5, 100)
    
    # Try to delete a non-existent key
    skiplist.delete(10)  # Should not raise any error
    
    # Verify the existing value is still there
    assert skiplist.search(5) == 100

def test_duplicate_inserts_comprehensive() -> None:
    skiplist = SkipList[str]()
    
    # Insert initial value
    skiplist.insert(1, "first")
    initial_size = skiplist.size
    
    # Insert duplicate with same size value
    skiplist.insert(1, "second")
    # Size should remain roughly the same (may have small differences due to string content)
    
    # Verify the value was updated
    assert skiplist.search(1) == "second"
    
    # Insert duplicate with different size value
    skiplist.insert(1, "much_longer_string_value")
    # Size should have increased
    larger_size = skiplist.size
    assert larger_size > initial_size
    
    # Verify the value was updated again
    assert skiplist.search(1) == "much_longer_string_value"
    
    # Insert duplicate with smaller value
    skiplist.insert(1, "short")
    smaller_size = skiplist.size
    assert smaller_size < larger_size
    
    # Verify the value was updated
    assert skiplist.search(1) == "short"
    
    # Test multiple consecutive duplicates
    for i in range(5):
        skiplist.insert(1, f"value_{i}")
    
    # Should have the last inserted value
    assert skiplist.search(1) == "value_4"
    
    # Insert other keys to ensure skiplist structure is intact
    skiplist.insert(2, "key2")
    skiplist.insert(3, "key3")
    
    # Verify all values are accessible
    assert skiplist.search(1) == "value_4"
    assert skiplist.search(2) == "key2"
    assert skiplist.search(3) == "key3"
    
    # Insert duplicate for key 2
    skiplist.insert(2, "updated_key2")
    
    # Verify all values are still accessible and correct
    assert skiplist.search(1) == "value_4"
    assert skiplist.search(2) == "updated_key2"
    assert skiplist.search(3) == "key3"