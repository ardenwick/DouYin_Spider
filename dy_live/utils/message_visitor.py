

from google.protobuf.message import Message
from google.protobuf.descriptor import FieldDescriptor
from typing import Any, Callable, List


def isRepeatedField(field) -> bool:
    """both map<> and repeated"""
    try:
        # TODO upgrade requirement of google protobuf to >= 7.x
        return field.is_repeated
    except AttributeError:
        return field.label == FieldDescriptor.LABEL_REPEATED


def _getMessageField(m: Message, path: str, result: List[Any]):
    if m is None or path == '':
        return
    while (m is not None) and len(path) > 0:
        dot = path.find('.')
        dot = len(path) if dot == -1 else dot
        field, path = path[:dot], path[dot + 1:]
        if field.endswith('[]'):
            for e in getattr(m, field[:-2]):
                _getMessageField(e, path, result)
            return
        elif field.endswith('{}'):
            for e in getattr(m, field[:-2]).values():
                _getMessageField(e, path, result)
            return
        else:
            if not m.HasField(field):  # field is not set
                return
            m = getattr(m, field)
    result.append(m)


def getMessageField(m: Message, path: str) -> List[Any]:
    result = []
    _getMessageField(m, path, result)
    return result


def getMessageFields(m: Message, paths: List[str]) -> List[Any]:
    result = []
    for path in paths:
        _getMessageField(m, path, result)
    return result


def visitObjectInMessage(m: Message, Ts: List[type], func: Callable) -> None:
    """
    Ts: object types
    """
    if type(m) in Ts:
        func(m)
        return
    if not isinstance(m, Message):
        return
    for field, value in m.ListFields():
        if isRepeatedField(field):
            if (field.message_type is not None) and field.message_type.GetOptions().map_entry:  # map<> field
                for k, v in value.items():
                    visitObjectInMessage(k, Ts, func)
                    visitObjectInMessage(v, Ts, func)
            else:  # repeated field
                for e in value:
                    visitObjectInMessage(e, Ts, func)
        else:
            visitObjectInMessage(value, Ts, func)


def getObjectFromMessageRecursive(m: Message, Ts: List[type]) -> List[Any]:
    result = []
    visitObjectInMessage(m, Ts, lambda o: result.append(o))
    return result
