
"""Searialize to and Desearilaize from JSON via msgspec."""
from __future__ import annotations

import decimal
import typing as t

import msgspec


def _default_encoding(obj: t.Any) -> t.Any:  # noqa: ANN401
    """Encoding helper for non native types.

    Args:
        obj: The object to encode.

    Returns:
        The encoded object.
    """
    return str(obj)

def _default_decoding(type: type, obj: t.Any) -> t.Any:  # noqa: ARG001, A002, ANN401
    """Decoding type helper for non native types.

    Args:
        type: the type given
        obj: the item to be decoded

    Returns:
        The object converted to the appropriate type, default is str.
    """
    return str(obj)

encoder = msgspec.json.Encoder(enc_hook=_default_encoding, decimal_format="number")
decoder = msgspec.json.Decoder(dec_hook=_default_decoding, float_hook=decimal.Decimal)

def deserialize_json(json_str: str | bytes, **kwargs: t.Any) -> dict:
    """Deserialize a line of json.

    Args:
        json_str: A single line of json.
        **kwargs: Optional key word arguments.

    Returns:
        A dictionary of the deserialized json.
    """
    return decoder.decode(json_str)


def serialize_json(obj: object, **kwargs: t.Any) -> str:
    """Serialize a dictionary into a line of json.

    Args:
        obj: A Python object usually a dict.
        **kwargs: Optional key word arguments.

    Returns:
        A string of serialized json.
    """
    return encoder.encode(obj).decode()

msg_buffer = bytearray(64)

def serialize_jsonl(obj: object, **kwargs: t.Any) -> bytes:
        """Serialize a dictionary into a line of jsonl.

        Args:
            obj: A Python object usually a dict.
            **kwargs: Optional key word arguments.

        Returns:
            A bytes of serialized json.
        """
        encoder.encode_into(obj, msg_buffer)
        msg_buffer.extend(b"\n")
        return msg_buffer
