import json

from lakefs.exceptions import ServerException

from lakefs_spec.errors import translate_lakefs_error


def test_error_translation() -> None:
    rpath = "repo/ref/ohno.txt"

    # first case: lakeFS API error, 401 unauthorized
    e = ServerException(
        status=401, reason="unauthorized", body=json.dumps({"message": "unauthorized"})
    )

    translated_err = translate_lakefs_error(e, rpath=rpath)
    assert isinstance(translated_err, PermissionError)
    assert f"unauthorized: {rpath!r}" in str(translated_err)

    # second case: lakeFS API error 420 (corresponds to partial IOError)
    e = ServerException(
        status=420, reason="too many requests", body=json.dumps({"message": "too many requests"})
    )
    translated_err = translate_lakefs_error(e, rpath=rpath)
    assert isinstance(translated_err, OSError)
    assert f"too many requests: {rpath!r}" in str(translated_err)

    # third case: lakeFS API error 400 with a custom message.
    e = ServerException(
        status=400, reason="bad request", body=json.dumps({"message": "bad request"})
    )
    message = "oh no!"
    translated_err = translate_lakefs_error(e, message=message)
    assert isinstance(translated_err, OSError)
    assert str(translated_err).endswith(message)
