class Sink:
    '''
    Abstract sink interface.

    A sink is responsible for persisting batches of records.
    '''

    async def write_batch(self, records: list[dict]) -> None:
        '''
        Persist a batch of records.

        Semtantics:
        - Called with a non-empty list of records
        - Must raise an exception on failure
        - Must not partially persist records
        '''

        raise NotImplementedError