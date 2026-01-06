class DeliveryError(Exception):
    '''Base class for delivery failures.'''
    pass

class RetryableDeliveryError(DeliveryError):
    '''Indicates a transient failure that may succeed upon retry.'''
    pass

class PermanentDeliveryError(DeliveryError):
    '''Indicates a non-recoverable failure.'''
    pass