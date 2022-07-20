from journalpump.senders.base import Tagged

import pytest


@pytest.mark.parametrize(
    "input_tags,expected_output_tags",
    [
        ({"foo": "bar"}, {"foo": "bar"}),
        ({"foo|wow,": "super!cool"}, {"foo_wow": "super!cool"}),
        ({"host": "localhost:1234"}, {"host": "localhost_1234"}),
        ({"base64": "YmFzZTY0Cg=="}, {"base64": "YmFzZTY0Cg"}),
        ({"source_address": "127.0.0.1"}, {"source_address": "127.0.0.1"}),
    ],
)
def test_tagged(input_tags: dict, expected_output_tags: dict):
    tagged = Tagged(None)
    assert tagged.make_tags(input_tags) == expected_output_tags
