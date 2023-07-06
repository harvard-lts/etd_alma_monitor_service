import tasks.tasks as tasks
import json

FEATURE_FLAGS = "feature_flags"


class TestTasksClass():

    def test_invoke_dims(self):
        message = {"unit_test": "true", FEATURE_FLAGS: {
                'dash_feature_flag': "off",
                'alma_feature_flag': "off",
                'send_to_drs_feature_flag': "off",
                'drs_holding_record_feature_flag': "off"}}
        json_args = json.dumps(message)
        retval = tasks.invoke_dims(json_args)
        assert "hello" in retval
        assert "feature_flags" in retval
