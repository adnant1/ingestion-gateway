from typing import Any, List
from pydantic import BaseModel, model_validator

class IngestRequest(BaseModel):
    '''
    Represents the raw ingestion payload.

    Accepted input shapes:
    - Single JSON object
    - List of JSON objects

    After validation:
    - Always normalized to List[dict]
    '''

    payload: List[dict[str, Any]]

    @model_validator(mode='before')
    @classmethod
    def normalize_payload(cls, data):
        payload = data.get('payload')

        # Reject missing payload
        if payload is None:
            raise ValueError('payload must not be empty')
        
        # Single object -> list
        if isinstance(payload, dict):
            data['payload'] = [payload]
            return data
        
        # List of objects
        if isinstance(payload, list):
            if not payload:
                raise ValueError('payload must not be empty')
            
            for item in payload:
                if not isinstance(item, dict):
                    raise ValueError('all records must be JSON objects')
                
            return data
        
        # Everything else is invalid
        raise ValueError('payload must be a JSON object or a list of JSON objects')

    def records(self) -> List[dict[str, Any]]:
        '''
        Returns the normalized list of records.
        '''
        return self.payload


class IngestResponse(BaseModel):
    '''
    Represents a successful ingestion acknowledgment.
    '''
    
    accepted_count: int
    message: str