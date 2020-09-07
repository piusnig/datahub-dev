import pandas as pd
import pytest
from moto import mock_s3, mock_ses

from .test_base import TestBase

from datahub_degree_validator.datahub_degree_validator import (
    File,
    BucketFileData,
    Settings,
    ValidateFile,
    lambda_handler,
    SendEmail,
)


# pytest --cov-report term-missing --cov=datahub_degree_validator tests/
# pytest -s
# isort tests/test_datahub_degree_validator/*.py datahub_degree_validator/datahub_degree_validator.py
# git rm --cached .vscode/settings.json


@mock_s3
@mock_ses
class TestLambdaHandler(TestBase):
    def test_lambda_handler_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        assert "Success" == lambda_handler(correct_files_event)

    def test_lambda_handler_WHEN_WrongFileEvent_THEN_None(self, wrong_files_event):
        assert None is lambda_handler(wrong_files_event)

    def test_lambda_handler_WHEN_wrong_file_names_event_THEN_tuple(
        self, wrong_file_names_event
    ):
        assert isinstance(lambda_handler(wrong_file_names_event), type(tuple()))

    def test_lambda_handler_WHEN_wrong_file_structure_event_THEN_tuple(
        self, wrong_file_structure_event
    ):
        assert isinstance(lambda_handler(wrong_file_structure_event), type(tuple()))

    def test_lambda_handler_WHEN_wrong_field_datatypes_event_THEN_tuple(
        self, wrong_field_datatypes_event
    ):
        assert isinstance(lambda_handler(wrong_field_datatypes_event), type(tuple()))

    def test_lambda_handler_WHEN_file_empty_event_THEN_tuple(self, file_empty_event):
        assert isinstance(lambda_handler(file_empty_event), type(tuple()))

    def test_lambda_handler_WHEN_file_pk_violation_event_THEN_tuple(
        self, file_pk_violation_event
    ):
        assert isinstance(lambda_handler(file_pk_violation_event), type(tuple()))

    def test_lambda_handler_WHEN_swap_files_event_THEN_Success(self, swap_files_event):
        assert "Success" == lambda_handler(swap_files_event)

    def test_lambda_handler_WHEN_bad_event_THEN_KeyError(self, bad_lambda_event):
        with pytest.raises(KeyError):
            lambda_handler(bad_lambda_event)


@mock_s3
class TestValidateCorrectFile(TestBase):
    def test_validate_lambda_event_WHEN_correct_files_event_THEN_file_path_list(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        assert file_path_list == ValidateFile(Settings()).validate_lambda_event(
            correct_files_event
        )

    def test_validate_lambda_event_event_WHEN_wrong_files_event_THEN_None(
        self, wrong_files_event
    ):
        assert None is ValidateFile(Settings()).validate_lambda_event(wrong_files_event)


@mock_s3
class TestValidateFile(TestBase):
    def test_validate_file_name_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        assert "Success" == ValidateFile(Settings()).validate_file_name(file_path_list)

    def test_validate_file_name_WHEN_wrong_file_names_event_THEN_tuple(
        self, wrong_file_names_event
    ):
        event_key = wrong_file_names_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))

        assert isinstance(
            ValidateFile(Settings()).validate_file_name(file_path_list), type(tuple())
        )

    def test_validate_file_name_WHEN_file_empty_event_THEN_tuple(
        self, file_empty_event
    ):
        event_key = file_empty_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert isinstance(
            ValidateFile(Settings()).validate_file_empty(file), type(tuple())
        )

    def test_validate_file_column_structure_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert "Success" == ValidateFile(Settings()).validate_file_column_structure(
            file
        )

    def test_validate_file_column_structure_WHEN_wrong_file_structure_event_THEN_tuple(
        self, wrong_file_structure_event
    ):
        event_key = wrong_file_structure_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert isinstance(
            ValidateFile(Settings()).validate_file_column_structure(file), type(tuple())
        )

    def test_validate_field_datatypes_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert "Success" == ValidateFile(Settings()).validate_field_datatypes(file)

    def test_validate_field_datatypes_WHEN_wrong_field_datatypes_event_THEN_tuple(
        self, wrong_field_datatypes_event
    ):
        event_key = wrong_field_datatypes_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert isinstance(
            ValidateFile(Settings()).validate_field_datatypes(file), type(tuple())
        )

    def test_validate_file_pk_violation_WHEN_correct_files_event_THEN_Success(
        self, correct_files_event
    ):
        event_key = correct_files_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert "Success" == ValidateFile(Settings()).validate_file_pk_violation(file)

    def test_validate_file_pk_violation_WHEN_file_pk_violation_event_THEN_tuple(
        self, file_pk_violation_event
    ):
        event_key = file_pk_violation_event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, event_key.split("/")))
        file = File(file_path_list)

        assert isinstance(
            ValidateFile(Settings()).validate_file_pk_violation(file), type(tuple())
        )


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


@mock_ses
class TestSendEmail(TestBase):
    def test_send_email(self, log):
        assert isinstance(SendEmail().send_email(log), type(dict()))

