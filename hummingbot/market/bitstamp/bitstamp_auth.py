import base64
import hashlib
import hmac
from hummingbot.market.bitstamp.bitstamp_tracking_nonce import get_tracking_nonce


class BitstampAuth:
    def __init__(self, client_id: str, api_key: str, secret_key: str):
        self.client_id = client_id
        self.api_key = api_key
        self.secret_key = secret_key

    def generate_auth_params(self) -> str:
        """
        Generates authentication signature and returns it in request parameters
        :return: a string of request parameters including the signature
        """

        # Decode API private key from base64 format displayed in account management
        api_secret: bytes = base64.b64decode(self.secret_key)
        # Get next nonce
        api_nonce: str = get_tracking_nonce()
        # Decode signature message to bytes
        sig_msg: bytes = base64.b64decode(api_nonce + self.client_id + self.api_key)

        # Cryptographic hash algorithms
        api_hmac: hmac.HMAC = hmac.new(api_secret, sig_msg, hashlib.sha256)

        # Encode signature into base64 format used in API-Sign value
        api_signature: bytes = base64.b64encode(api_hmac.digest())

        return f"key={self.api_key}&signature={str(api_signature, 'utf-8')}&nonce={api_nonce}"
