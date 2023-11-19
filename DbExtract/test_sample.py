"""
You can auto-discover and run all tests with this command:

    pytest

Documentation: https://docs.pytest.org/en/latest/

pytest will run all files of the form test_*.py or *_test.py in the 
current directory and its subdirectories. 
More generally, it follows standard test discovery rules.
"""


def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 4
