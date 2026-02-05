from airflow.hooks.base import BaseHook
import requests 


# class TfLHook(BaseHook):
#     """
#     Hook for interacting with the Transport for London (TFL) API.

#     :param api_key: TFL API key 
#     :type api_key: str
#     """ 

#     def __init__(self, api_key: str) -> None:
#         super().__init__()
#         self.api_key = api_key
#         self.base_url = "https://api.tfl.gov.uk" 

#     def get_line_status(self, line_id: str) -> dict:
#         """
#         Get the status of a specific TFL line.

#         :param line_id: The ID of the TFL line (e.g., 'victoria', 'central').
#         :type line_id: str
#         :return: A dictionary containing the line status information.
#         :rtype: dict
#         """
#         endpoint = f"/Line/{line_id}/Status"
#         params = {"app_key": self.api_key} 
#         response = requests.get(f"{self.base_url}{endpoint}", params=params)
#         response.raise_for_status() 
#         return response.json() 
    




class TfLHook(BaseHook):
    def __init__(self, tfl_conn_id='tfl_default'):
        super().__init__()
        self.tfl_conn_id = tfl_conn_id

    def get_data(self, endpoint, params=None):
        # Lấy Key từ Airflow Variable (Admin -> Variables trên UI)
        from airflow.models import Variable
        api_key = Variable.get("tfl_app_key") 
        
        base_url = f"https://api.tfl.gov.uk/{endpoint}"
        payload = params or {}
        payload['app_key'] = api_key
        
        response = requests.get(base_url, params=payload)
        response.raise_for_status() # Báo lỗi nếu API gặp sự cố
        return response.json() # Trả về Root (List hoặc Dict)