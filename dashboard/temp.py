


with open('user_account.txt', 'r') as file:
    account, user, password = file.readlines()
    account = account.rstrip()
    user = user.rstrip()

print(account, user ,password)