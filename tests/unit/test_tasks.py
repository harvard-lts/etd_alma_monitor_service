import tasks.tasks as tasks

FEATURE_FLAGS = "feature_flags"


class TestTasksClass():

    def test_invoke_dims(self):
        message = {"unit_test": "true", FEATURE_FLAGS: {
                'dash_feature_flag': "off",
                'alma_feature_flag': "off",
                'send_to_drs_feature_flag': "off",
                'drs_holding_record_feature_flag': "off"}}
        retval = tasks.invoke_dims(message)
        assert "hello" in retval
        assert "feature_flags" in retval
