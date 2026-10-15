import re

import dy_live.protobuf.douyin.bizIm.webcast.data_pb2 as data
import dy_live.protobuf.douyin.bizIm.webcast.im_pb2 as im
from dy_live.utils.message_visitor import getMessageField


def str_public_area_badges(m: data.User):
    m = m.public_area_badge_info
    badges = []
    for i in m.badge_list:
        b: str = m.badge_info_map[i].content.alternative_text
        b = b.replace('等级', '').removesuffix('勋章')
        if len(b) == 0:
            if m.badge_info_map[i].image_type == 59:  # 会员勋章
                b = '会员'
            if 'yearly' in m.badge_info_map[i].uri:
                b = '年' + b
        badges.append(str(b))
    return ' '.join(badges)


def format_text_piece_user(t: data.TextPieceUser, **kwargs) -> str:
    '''
    kwargs:
        user_format:
            '<user_format>'.format(words below)
            Default "({badges}) {.nickname}\u200b"

    Available words:
        {nickname}          : {.nickname}
        {id}                : {.id}
        {pay_grade_level}   : {.pay_grade.level}
        {fans_club_level}   : {.fans_club.data.level}
        {badges}            : "勋章列表..."
    '''
    predefined = {
        r'{nickname}': 'nickname',
        r'{id}': 'id',
        r'{pay_grade_level}': 'pay_grade.level',
        r'{fans_club_level}': 'fans_club.data.level',
    }

    u: data.User = t.user

    if 'user_format' in kwargs:
        user_format: str = kwargs['user_format']

        result = user_format
        for m in re.findall(r'\{[^}]+\}', user_format):
            path = ''
            if m == r'{badges}':
                result = result.replace(r'{badges}', str_public_area_badges(u))
                continue
            elif m in predefined:
                path = predefined[m]
            elif m.startswith('{.'):  # {.some.path}
                path = m[2:-1]
            else:
                continue  # leave it as-is
            field = (getMessageField(u, path) or [''])[0]
            result = result.replace(m, str(field))
        return result

    elif u.public_area_badge_info.badge_list:
        return f'({str_public_area_badges(u)}) {u.nickname}\u200b'
    else:
        return f'{u.nickname}\u200b({u.id})'


def format_text_piece_string(t: str, **kwargs) -> str:
    return t


def format_text_piece_image(t: data.TextPieceImage, **kwargs) -> str:
    r = ''
    if t.image.url_list:
        r = '[image](' + t.image.url_list[0] + ')'
    return r


def format_text_piece_gift(t: data.TextPieceGift, **kwargs) -> str:
    return f'{t.name_ref.default_pattern}(ID {t.gift_id})'


formatters = {
    'user': format_text_piece_user,
    'string': format_text_piece_string,
    '': format_text_piece_string,
    'image': format_text_piece_image,
    'gift': format_text_piece_gift,
}


# renderer for webcast.data.Text
def render_text(text: data.Text, **kwargs) -> str:
    pattern: str = text.default_pattern
    pieces = text.pieces
    result = pattern
    for m in re.findall(r'\{[^}]+\}', pattern):
        if ':' in m:
            i, k = m[1:-1].split(':')
        else:
            i, k = m[1:-1], 'string'
        v = getattr(pieces[int(i)], k + '_value')

        t = formatters[k](v, **kwargs)

        result = result.replace(m, t)
    return result
