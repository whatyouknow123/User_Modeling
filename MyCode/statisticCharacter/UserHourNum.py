from MyCode.createTable import sql_functions
def main():
    conn, cursor = sql_functions.connect_database()
    sql_value = ('select userid, from_unixtime(submittime, "%k") as date_hour, '
                 'count(*) as number from basic_table group by userid, from_unixtime(submittime, "%k");')
    results = sql_functions.search_table(sql_value, cursor)
    # get the user hour task number
    users = {}
    for result in results:
        userid = int(result[0])
        date_hour = int(result[1])
        hour_number = int(result[2])
        print userid
        print date_hour
        print hour_number
        if userid in users.keys():
            tempvalue = users[userid]
            tempvalue[date_hour] = hour_number
            users[userid] = tempvalue
        else:
            tempvalue = [0]*24
            tempvalue[date_hour] = hour_number
            users[userid] = tempvalue

    for each in users.keys():
        tempvalue = users[each]
        sql_value2 = ('insert into user_day_hour VALUES (%d, %d, %d, %d, %d,'
                     '%d, %d, %d, %d, %d,%d, %d, %d, %d, %d,%d, %d, %d, %d, %d,'
                     '%d, %d, %d, %d, %d)') % (each, tempvalue[0], tempvalue[1], tempvalue[2], tempvalue[3],
                                               tempvalue[4], tempvalue[5], tempvalue[6], tempvalue[7],
                                               tempvalue[8], tempvalue[9], tempvalue[10], tempvalue[11],
                                               tempvalue[12], tempvalue[13], tempvalue[14], tempvalue[15],
                                               tempvalue[16], tempvalue[17], tempvalue[18], tempvalue[19],
                                               tempvalue[20], tempvalue[21], tempvalue[22], tempvalue[23])

        sql_functions.insert_table(sql_value2, cursor, conn)
    sql_functions.close_database(conn)

if __name__ == "__main__":
    main()




