from auth import register_user, login_user

def validate_username(username):
    if not username.isalnum() or not (3 <= len(username) <= 20):
        return False, "Username must be 3–20 alphanumeric characters."
    return True, ""

def validate_password(password):
    if not (6 <= len(password) <= 50):
        return False, "Password must be 6–50 characters long."
    return True, ""

def display_menu():
    print("\n" + "="*50)
    print(" MULTI-DOMAIN INTELLIGENCE PLATFORM")
    print(" Secure Authentication System")
    print("="*50)
    print("\n [1] Register a new user")
    print(" [2] Login")
    print(" [3] Exit")
    print("-"*50)

def main():
    print("\nWelcome to the Week 7 Authentication System!")
    while True:
        display_menu()
        choice = input("\nPlease select an option (1-3): ").strip()
        if choice == '1':
            print("\n--- USER REGISTRATION ---")
            username = input("Enter a username: ").strip()
            is_valid, error = validate_username(username)
            if not is_valid:
                print(f"Error: {error}")
                continue
            password = input("Enter a password: ").strip()
            is_valid, error = validate_password(password)
            if not is_valid:
                print(f"Error: {error}")
                continue
            confirm = input("Confirm password: ").strip()
            if password != confirm:
                print("Error: Passwords do not match.")
                continue
            register_user(username, password)

        elif choice == '2':
            print("\n--- USER LOGIN ---")
            username = input("Enter your username: ").strip()
            password = input("Enter your password: ").strip()
            login_user(username, password)
            input("\nPress Enter to return to main menu...")

        elif choice == '3':
            print("\nThank you for using the authentication system.")
            break
        else:
            print("❌ Invalid option. Please select 1, 2, or 3.")

if __name__ == "__main__":
    main()