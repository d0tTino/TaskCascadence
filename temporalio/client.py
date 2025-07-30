class Client:
    async def execute_workflow(self, *args, **kwargs):
        pass

    @classmethod
    async def connect(cls, server: str):
        return cls()
