from attrs import define


@define
class tidyworkflow:
    def __enter__(self):
        print("Starting")
        return self

    def __exit__(self, *exc):
        print("Finishing")
        return False
