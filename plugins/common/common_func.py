def get_stfp():
    print("start stfp")


def regist(name, sex, *args):
    print(f'name: {name}, sex: {sex}')
    print(f'args: {args}')


def regist2(name, sex, *args, **kwargs):
    print(f'name: {name}')
    print(f'sex: {sex}')
    print(f'args: {args}')
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None
    if email:
        print(f'email: {email}')
    if phone:
        print(f'phone: {phone}')