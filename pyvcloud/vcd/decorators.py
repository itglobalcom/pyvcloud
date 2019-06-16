from functools import wraps


def simple_call(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        result = await method(self, *args, **kwargs)
        try:
            task = result.Tasks.Task[0]
            returned_id = result.get('id')
        except AttributeError:
            task = result
            try:
                returned_id = task.get('id')
            except AttributeError:
                returned_id = None
        await self.get_task_monitor().wait_for_success(
            task=task
        )
        return returned_id
    return wrapper
