from functools import wraps


def simple_call(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        result = await method(self, *args, **kwargs)
        if hasattr(result, 'tag') and result.tag.endswith('}ScreenTicket'):
            return result
        elif hasattr(result, 'tag') and result.tag.endswith('}MksTicket'):
            return result
        else:
            try:
                task = result.Tasks.Task[0]
                returned_id = result.get('id')
            except AttributeError:
                task = result
                returned_id = None
            await self.get_task_monitor().wait_for_success(
                task=task,
                timeout=3 * 60 * 60
            )
            return returned_id
    return wrapper
