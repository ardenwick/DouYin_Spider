from enum import Enum

class LotteryCondition(Enum):
    发送评论 = 3  # Comment
    赠送礼物 = 4  # SendGift
    加入粉丝团 = 5  # JoinFansClub
    默认参与 = 6  # DefaultParticipate
    粉丝团等级 = 7  # FansClubLevel
    点亮粉丝团 = 8  # LightFansClub
    分享直播间 = 9  # ShareRoom
    助力心愿单 = 10  # HelpWish
    会员 = 11  # Vip
    自定义 = 50  # Custom
    唱歌 = 51  # Song
    互动 = 52  # Interact