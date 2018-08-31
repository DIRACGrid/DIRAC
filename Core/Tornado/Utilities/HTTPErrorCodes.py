"""
    List of HTTP codes

    https://tools.ietf.org/html/rfc2616#section-6.1.1
"""

# Success code
HTTP_OK = 200


# Client error code
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
HTTP_IM_A_TEAPOT = 418  # see https://tools.ietf.org/html/rfc2324


# Server error code
HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_NOT_IMPLEMENTED = 501
