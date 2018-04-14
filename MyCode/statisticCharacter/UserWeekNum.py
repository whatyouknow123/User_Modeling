from MyCode.createTable import sql_functions
def main():
    conn, cursor = sql_functions.connect_database()
    sql_value = 'select * from user_week_view'
    results = sql_functions.search_table(sql_value, cursor)
    # get the user task number in every day of a week
    users = {}
    for result in results:
        userid = int(result[0])
        date_week = int(result[1])
        hour_number = int(result[2])
        print userid
        print date_week
        print hour_number
        if userid in users.keys():
            tempvalue = users[userid]
            tempvalue[date_week] = hour_number
            users[userid] = tempvalue
        else:
            tempvalue = [0] * 7
            tempvalue[date_week] = hour_number
            users[userid] = tempvalue

    for each in users.keys():
        tempvalue = users[each]
        sql_value2 = ('insert into user_week_day VALUES (%d, %d, %d, %d, %d,'
                      '%d, %d, %d)') % (each, tempvalue[0], tempvalue[1], tempvalue[2], tempvalue[3],
                                        tempvalue[4], tempvalue[5], tempvalue[6])

        sql_functions.insert_table(sql_value2, cursor, conn)
    sql_functions.close_database(conn)


if __name__ == "__main__":
    main()
