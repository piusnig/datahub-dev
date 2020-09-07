from moto import mock_s3, mock_ses


@mock_s3
@mock_ses
class TestBase:
    def test_set_up(self, conns):
        conns()
