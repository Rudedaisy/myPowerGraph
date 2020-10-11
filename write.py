f = open("simgridfile.cpp", "w")

host = ""
host += "line 1()\n"
host += "\tline2\n"

f.write("prefix\n")
f.write(host)
f.write("suffix\n")
f.close()
