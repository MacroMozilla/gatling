from gatling.utility.io_fctns import *

save_json({"a": 1}, "data.json")
print(read_json("data.json"))
remove_file("data.json")

save_jsonl([{"x": 1}, {"x": 2}], "data.jsonl")
print(read_jsonl("data.jsonl"))

remove_file("data.jsonl")

save_text("Hello world", "msg.txt")
print(read_text("msg.txt"))

remove_file("msg.txt")
