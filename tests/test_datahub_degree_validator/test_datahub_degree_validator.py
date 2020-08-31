import pandas as pd
import pytest
from moto import mock_s3

from .test_base import TestBase

from datahub_degree_validator.datahub_degree_validator import (  # LogInfo,; SendEmail,
    File,
    BucketFileData,
    Settings,
    ValidateFile,
    lambda_handler,
    # ErrorLogging,
)


# pytest --cov-report term-missing --cov=datahub_degree_validator tests/
# pytest -s
# isort tests/test_datahub_degree_validator/*.py datahub_degree_validator/datahub_degree_validator.py


@mock_s3
class TestLambdaHandler(TestBase):
    def test_lambda_handler_WHEN_correct_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        print(lambda_handler(correct_files_event))
        assert "Success" == lambda_handler(correct_files_event)

    def test_lambda_handler_WHEN_WrongFileEvent_THEN_None(self, wrong_files_event):
        assert None is lambda_handler(wrong_files_event)

    def test_lambda_handler_WHEN_wrong_file_names_event_THEN_dict(
        self, wrong_file_names_event
    ):
        assert isinstance(lambda_handler(wrong_file_names_event), type(dict()))

    def test_lambda_handler_WHEN_wrong_file_structure_event_THEN_dict(
        self, wrong_file_structure_event
    ):
        assert isinstance(lambda_handler(wrong_file_structure_event), type(dict()))

    def test_lambda_handler_WHEN_wrong_field_datatypes_event_THEN_dict(
        self, wrong_field_datatypes_event
    ):
        assert isinstance(lambda_handler(wrong_field_datatypes_event), type(dict()))

    def test_lambda_handler_WHEN_file_empty_event_THEN_dict(self, file_empty_event):
        assert isinstance(lambda_handler(file_empty_event), type(dict()))

    def test_lambda_handler_WHEN_file_pk_violation_event_THEN_dict(
        self, file_pk_violation_event
    ):
        assert isinstance(lambda_handler(file_pk_violation_event), type(dict()))

    def test_lambda_handler_WHEN_bad_event_THEN_KeyError(self, bad_lambda_event):
        with pytest.raises(KeyError):
            lambda_handler(bad_lambda_event)


@mock_s3
class TestValidateCorrectFile(TestBase):
    def test_validate_lambda_event_WHEN_correct_files_event_THEN_file_path_list(
        self, correct_files_event
    ):
        settings = Settings()
        validate_file = ValidateFile()
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        assert file_path_list == validate_file.validate_lambda_event(
            correct_files_event, settings
        )

    def test_validate_lambda_event_event_WHEN_wrong_files_event_THEN_None(
        self, wrong_files_event
    ):
        settings = Settings()
        validate_file = ValidateFile()
        assert None is validate_file.validate_lambda_event(wrong_files_event, settings)


@mock_s3
class TestValidateFile(TestBase):
    def test_validate_file_name_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):

        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        validate_file = ValidateFile()
        settings = Settings()
        metadata = settings.get_metadata()

        assert "Success" == validate_file.validate_file_name(file_path_list, metadata)

    def test_validate_file_name_WHEN_wrong_file_names_event_THEN_Failed(
        self, wrong_file_names_event
    ):

        event_key = wrong_file_names_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        validate_file = ValidateFile()
        settings = Settings()
        metadata = settings.get_metadata()
        assert isinstance(
            validate_file.validate_file_name(file_path_list, metadata), type(dict())
        )

    def test_validate_file_column_structure_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        validate_file = ValidateFile()
        settings = Settings()
        metadata = settings.get_metadata()
        file = File(file_path_list)

        assert "Success" == validate_file.validate_file_column_structure(file, metadata)

    def test_validate_file_column_structure_WHEN_wrong_file_structure_event_THEN_Failed(
        self, wrong_file_structure_event
    ):
        event_key = wrong_file_structure_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        validate_file = ValidateFile()
        settings = Settings()
        metadata = settings.get_metadata()
        file = File(file_path_list)
        assert isinstance(
            validate_file.validate_file_column_structure(file, metadata), type(dict())
        )

    def test_validate_field_datatypes_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        validate_file = ValidateFile()
        settings = Settings()
        file = File(file_path_list)
        settings.set_file_settings()
        assert "Success" == validate_file.validate_field_datatypes(file, settings)

    def test_validate_field_datatypes_WHEN_wrong_field_datatypes_event_THEN_dict(
        self, wrong_field_datatypes_event
    ):
        event_key = wrong_field_datatypes_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        validate_file = ValidateFile()
        settings = Settings()
        file = File(file_path_list)
        settings.set_file_settings(file.file_path_no_ext)
        assert isinstance(
            validate_file.validate_field_datatypes(file, settings), type(dict())
        )

    # def test_validate_file_pk_violation_WHEN_correct_files_event_THEN_Success(
    #     self, correct_files_event
    # ):
    #     event_key = correct_files_event["detail"]["requestParameters"]["key"]
    #     file_path_list = list(map(str.lower, event_key.split("/")))

    #     validate_file = ValidateFile()
    #     settings = Settings()
    #     file = File(file_path_list)
    #     settings.set_file_settings()
    #     assert "Success" == validate_file.validate_file_pk_violation(file, settings)


@mock_s3
class TestBucketFileData(TestBase):
    def test_read_csv_WHEN_correct_file_event_THEN_data_frame(
        self, correct_files_event, partner_bucket
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        assert isinstance(
            BucketFileData().read_csv(partner_bucket, event_key), type(pd.DataFrame())
        )

    def test_read_csv_WHEN_wrong_file_names_event_THEN_Exception(
        self, wrong_file_names_event, partner_bucket
    ):
        event_key = wrong_file_names_event["detail"]["requestParameters"]["key"]
        assert None is BucketFileData().read_csv(partner_bucket, event_key)

    def test_read_csv_WHEN_wrong_file_structure_event_THEN_DataFrame(
        self, wrong_file_structure_event, partner_bucket
    ):
        event_key = wrong_file_structure_event["detail"]["requestParameters"]["key"]
        assert isinstance(
            BucketFileData().read_csv(partner_bucket, event_key), type(pd.DataFrame())
        )

    def test_read_csv_WHEN_file_not_exists_event_THEN_DataFrame(
        self, file_not_exists_event, partner_bucket
    ):
        event_key = file_not_exists_event["detail"]["requestParameters"]["key"]
        assert None is BucketFileData().read_csv(partner_bucket, event_key)

    # def test_upload_csv_WHEN_file_not_exists_event_THEN_DataFrame(
    #     self, file_not_exists_event
    # ):
    #     event_key = file_not_exists_event["detail"]["requestParameters"]["key"]
    #     assert 404 == BucketFileData().read_csv(partner_bucket, event_key)
    #     bfd = BucketFileData()
    #     file_df = bfd.read_csv("oaljadda", "datahub_validator/settings/out.csv")

    #     bfd.upload_csv("oaljadda", file_df, "datahub_validator/settings/out.csv")

    # def test_upload_csv_WHEN_file_not_exists_event_THEN_DataFrame(
    #     self, file_empty_event
    # ):
    #     event_key = file_not_exists_event["detail"]["requestParameters"]["key"]
    #     assert 404 == BucketFileData().read_csv(partner_bucket, event_key)
    #     bfd = BucketFileData()


@mock_s3
class TestSettings(TestBase):
    def test_get_metadata_WHEN_correct_file_event_THEN_data_frame(
        self, correct_files_event
    ):
        assert isinstance(Settings().get_metadata(), type(pd.DataFrame()))

    def test_get_fieldnames_WHEN_correct_file_event_THEN_data_frame(
        self, correct_files_event
    ):
        assert isinstance(Settings().get_fieldnames(), type(pd.DataFrame()))


@mock_s3
class TestErrorLogging(TestBase):
    # def test_log_info_wrong_file_name_WHEN_wrong_files_event_THEN_data_frame(
    #     self, wrong_file_names_event
    # ):
    #     event_key = wrong_file_names_event["detail"]["requestParameters"]["key"]
    #     settings = Settings()
    #     settings.get_partner_schedule()
    #     error_logging.log_info_wrong_file_name(file, error_log, log)
    #     error_logging.add_logs_to_bucker(logs_bucket, log)

    def test_add_logs_to_bucker_WHEN_correct_file_event_THEN_data_frame(self):
        # error_logging = ErrorLogging()
        pass
