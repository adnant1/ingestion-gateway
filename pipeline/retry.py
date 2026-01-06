import asyncio
from typing import Callable, Awaitable

from pipeline.errors import PermanentDeliveryError
from pipeline.errors import RetryableDeliveryError

class RetryPolicy:
    '''
    Controls retry behavior for batch delivery.
    '''

    def __init__(
            self, 
            max_attempts: int,
            base_delay: float,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
    
    async def execute(
            self,
            operation: Callable[[], Awaitable[None]],
    ):
        '''
        Execute an async operation with retries.

        Semantics:
        - Retry on RetryableDeliveryError
        - Abort immediately on PermanentDeliveryError
        - Raise after max_attempts
        '''

        attempt = 0
        while attempt < self.max_attempts:
            try:
                await operation()
                return
            except RetryableDeliveryError:
                attempt += 1
                if attempt >= self.max_attempts:
                    raise
                await asyncio.sleep(self.base_delay * (2 ** (attempt - 1)))
            except PermanentDeliveryError:
                raise