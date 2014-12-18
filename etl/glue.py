import sys
import mysql_io

def main():
  mysql_pwd = sys.argv[1]

  con = mysql_io.new_con("localhost", "root", mysql_pwd, "transformed")
  into_outfile(con, , f, table)

if __name__ == "__main__":
  main()
