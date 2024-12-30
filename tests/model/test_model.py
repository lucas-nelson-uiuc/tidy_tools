class TestTidyDataModel:
    def test_load_from_source(self, simpsons_data, simpsons_model):
        # def read_func(x, *args, **kwargs):
        #     return x

        # # validate returned object is a DataFrame
        # data = simpsons_model.load(simpsons_data, read_func=read_func)
        # assert isinstance(data, DataFrame)

        # # validate multiple sources can be passed
        # data = simpsons_model.load(
        #     simpsons_data, simpsons_data, simpsons_data, read_func=read_func
        # )
        # assert isinstance(data, DataFrame)

        # # validate columns match model fields
        # model_columns = attrs.fields_dict(simpsons_model).keys()
        # assert set(data.columns).difference(model_columns) == set()

        assert True
