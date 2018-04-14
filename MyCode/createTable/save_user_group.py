"""
    save the (userid, groupid, teamid) into the user_group table in the database named usermodel
"""
import sql_functions

def save_data(file):
    """
    read data from file and save data into the user_group table
    :param file:
    :return:
    """
    conn, cursor = sql_functions.connect_database()
    with open(file, "r") as fread:
        lines = fread.readlines()
        for line in lines:
            tempresult = line.strip().split()
            userid = tempresult[0]
            groupid = tempresult[1]
            teamid = tempresult[2]
            print userid, groupid,teamid
            sql = "insert into user_group VALUES (%d, %d, %d)" % (int(userid), int(groupid), int(teamid))
            cursor.execute(sql)
            conn.commit()
    conn.close()
    print "insert all successfully!"

def main():
    save_data("F:/usermodel/uid_gid_teamid.txt")

if __name__ == "__main__":
    main()