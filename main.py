from login import get_logged_in_client

username = "jennyye__"

cl = get_logged_in_client()
user_info = cl.user_info_by_username(username)
print(user_info.username, user_info.follower_count)
cl.direct_send('How are you?', user_ids=[user_info.pk])
