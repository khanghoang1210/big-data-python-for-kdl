import random

def name_ran_func(num):
    name_list = []
    i = 0
    ran_val = None

    while i < num:
        ran_val = random.randint(1, 5000)
        if ran_val not in name_list:
            name_list.append(str(ran_val))
            
            i -= -1

    return name_list


def rankandgross_change_rand_func(num):
    rank_change_list = []
    gross_change_list = []
    i = 0
    ran_val = None

    while i < num:
        ran_val = round(random.uniform(-10, 10), 2)
        rank_change_list.append(ran_val)
        ran_val = round(random.uniform(-20, 20), 2)
        gross_change_list.append(ran_val)

        i -= -1

    return rank_change_list, gross_change_list


def revenue_ran_func(num):
    revenue_list = []
    i = 0
    ran_val = None

    while i < num:
        ran_val = random.randrange(-100000, 2900000000)
        revenue_list.append(ran_val)

        i -= -1

    return revenue_list


def rating_ran_func(num):
    rating_list = []
    i = 0
    ran_val = None

    while i < num:
        ran_val = round(random.uniform(2.0, 5.0), 2)
        rating_list.append(ran_val)

        i -= -1

    return rating_list


def main():
    num = 1000

    name_list = name_ran_func(num)
    rank_change_list, gross_change_list = rankandgross_change_rand_func(num)
    revenue_list = revenue_ran_func(num)
    rating_list = rating_ran_func(num)

    with open('data.csv', mode='w', encoding='utf-8') as file:
        file.write('name,rank_change,gross_change,revenue,rating\n')

        for i in range(num):
            file.write(f'{name_list[i]},{rank_change_list[i]},{gross_change_list[i]},{revenue_list[i]},{rating_list[i]}\n')


main()
