import bcrypt
import os

USER_DATA_FILE = "users.txt"

def hash_password(plain_text_password):
    password_bytes = plain_text_password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    return hashed.decode('utf-8')

def verify_password(plain_text_password, hashed_password):
    password_bytes = plain_text_password.encode('utf-8')
    hashed_bytes = hashed_password.encode('utf-8')
    return bcrypt.checkpw(password_bytes, hashed_bytes)

def user_exists(username):
    if not os.path.exists(USER_DATA_FILE):
        return False
    with open(USER_DATA_FILE, "r") as f:
        for line in f:
            stored_username, _ = line.strip().split(",")
            if stored_username == username:
                return True
    return False

def register_user(username, password):
    if user_exists(username):
        print(f" Error: Username '{username}' already exists.")
        return False
    hashed = hash_password(password)
    with open(USER_DATA_FILE, "a") as f:
        f.write(f"{username},{hashed}\n")
    print(f" Success: User '{username}' registered successfully!")
    return True

def login_user(username, password):
    if not os.path.exists(USER_DATA_FILE):
        print(" No users registered yet.")
        return False
    with open(USER_DATA_FILE, "r") as f:
        for line in f:
            stored_username, stored_hash = line.strip().split(",")
            if stored_username == username:
                if verify_password(password, stored_hash):
                    print(f" Success: Welcome, {username}!")
                    return True
                else:
                    print(" Error: Invalid password.")
                    return False
    print(" Error: Username not found.")
    return False