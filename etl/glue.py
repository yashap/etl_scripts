import sys
import mysql_io

def main():
  mysql_pwd = sys.argv[1]

  con = mysql_io.new_con("localhost", "root", mysql_pwd, "etl_demo")
  f = mysql_io.outfile_name("member")
  mysql_io.into_outfile(con, "/home/ubuntu/etl_scripts/etl", f, "member")

if __name__ == "__main__":
  main()



