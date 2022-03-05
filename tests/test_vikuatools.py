from vikuatools.utils import int_to_string

def test_int_to_string():
	""" Test util function"""
	expected = '2'
	actual = int_to_string(2)

	assert actual == expected, 'Error in test int_to_string!'
