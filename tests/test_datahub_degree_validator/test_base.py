from moto import mock_s3


@mock_s3
class TestBase:
    def test_set_up(self, moto_boto):
        moto_boto()
