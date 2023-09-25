import re

# import chardet
from sql_keywords import sql_keywords


class OwiLanceSqlLinter:
    def __init__(self):
        self.file = None
        self.all_messages = []
        self.raw_messages = []
        self.lines = []
        self.csv_data = []
        self.in_declare_block = False
        self.declare_block_started = False

        # Check Tabulation
        self.tabulation_check = False
        self.tabulation_check_list = {}
        # Check Exist
        self.exist_check = False
        self.exist_check_list = {}
        # Check byte
        self.byte_check = False
        self.byte_check_list = {}
        # Check variable
        self.check_variable = False
        self.check_variable_list = {}
        # Check empty comment
        self.check_empty_comment = False
        self.check_empty_comment_list = {}
        # Check naming convention
        self.check_naming_convention = False
        self.check_naming_convention_list = {}
        # Check parameter convention
        self.check_parameter_convention = False
        self.check_parameter_convention_list = {}
        # Check sql keywords
        self.check_sql_keywords = False
        self.check_sql_keywords_list = {}
        # Check on bul for int
        self.check_on_bulk_for_in = False
        self.check_on_bulk_for_in_list = {}
        # Check on NVL
        self.check_on_nvl = False
        self.check_on_nvl_list = {}
        # Check on USING 
        self.check_on_using = False
        self.check_on_using_list = {}
 
    def check_if_file_is_ansi(self, file_path):
        num_bytes=1024
        check_ansi = True
        try:
            with open(file_path, 'rb') as file:
                data = file.read(num_bytes)
                # Check if all bytes are within the range [0, 255]
                check_ansi = all(0 <= byte <= 255 for byte in data)
        except FileNotFoundError:
            check_ansi = False
        
        self.append_message(
            "0",
            "FILE IS ANSI ENCODED",
            not (check_ansi),
            lines_data=None,
        )
        
    def open_files_and_get_lines(self, file_path):
        try:
            with open(file_path, "r", encoding="ISO-8859-1") as file:
                print("Successfully opened the file with UTF-8 encoding: " + file_path)
                self.lines = file.readlines()
        except UnicodeDecodeError:
            try:
                with open(file_path, "r", encoding="cp1252") as file:
                    print(
                        "Successfully opened the file with cp1252 encoding: "
                        + file_path
                    )
                    self.lines = file.readlines()
            except UnicodeDecodeError:
                print(
                    "Error: Unable to decode the file with either UTF-8 or cp1252 encoding."
                )

    def append_message(
        self,
        check_number,
        custom_message,
        failed_condition,
        lines_data=None,
    ):
        internal_custom_message = ""
        number_of_error = 0
        if failed_condition:
            number_of_error = len(lines_data.keys()) if lines_data else 1
            msg = f"❌ Check FAIL {check_number}\n\t {custom_message} \n\n\t{str(number_of_error)} ERRORS"
            self.all_messages.append(msg)
            self.raw_messages.append(msg)
            if lines_data:
                lines_data = "\n".join(
                    [
                        "\t -" + line
                        for line in [
                            f"{key}: {value}" for key, value in lines_data.items()
                        ]
                    ]
                )
                internal_custom_message = f"\tFailed lines: \n{lines_data}"
                self.all_messages.append(internal_custom_message)
        else:
            msg = f"✅ Check {check_number} PASS\n\t{custom_message}"
            self.all_messages.append(msg)
            self.raw_messages.append(msg)

        status = "✅ PASS" if not failed_condition else "❌FAIL"
        self.csv_data.append(
            [
                f"Check {check_number}",
                custom_message,
                status,
                str(number_of_error),
                internal_custom_message,
            ]
        )

    def u_check_rentrance(self):
        try:
            first_line = self.lines[0].strip()
        except Exception:
            first_line = None
        self.append_message(
            "1",
            "RENTRANCE=(ON/OFF) must be on top",
            first_line not in ["-- REENTRANCE = OFF", "-- REENTRANCE = ON"],
            lines_data=None,
        )

    def u_check_fin_du_script(self):
        try:
            last_line = self.lines[-1].strip()
        except Exception:
            last_line = None
        self.append_message(
            "2",
            "--Fin du script must be at the end of the SQL file",
            not (last_line == "-- Fin du script"),
            lines_data=None,
        )

    def u_check_show_error(self):
        try:
            avant_dernier = self.lines[-2].strip()
        except Exception:
            avant_dernier = None
        self.append_message(
            "3",
            "SHOW ERR; must be just before --Fin du script",
            not (avant_dernier == "SHOW ERR;"),
            lines_data=None,
        )
    
    def u_check_end(self):
        try:
            line_min3 = self.lines[-3].strip()
            line_min4 = self.lines[-4].strip()
        except Exception:
            line_min3 = None
            line_min4 = None
        self.append_message(
            "4",
            "END; / must be just before SHOW ERRR;",
            not (line_min3 == "/" and line_min4 == "END;"),
            lines_data=None,
        )

    def f_check_tabulation(self, line_number, line):
        if "\t" in line:
            self.tabulation_check = True
            self.tabulation_check_list[f"❗ L{line_number+1}"] = line.strip()

    def f_check_exist(self, line_number, line):
        if "exist(" in line.strip().lower():
            self.exist_check = True
            self.exist_check_list[f"❗ L{line_number+1}"] = line.strip()

    def f_check_varcharbyte(self, line_number, line):
        match = re.search(r"varchar\d?\(\d+\s+BYTE\)", line, re.IGNORECASE)
        if match:
            self.byte_check = True
            self.byte_check_list[f"❗ L{line_number+1}"] = line.strip()

    def f_check_declare_begin(self, line_number, line):
        declare_begin = line.strip().lower()
        if declare_begin == "declare":
            self.declare_block_started = True
            self.in_declare_block = False
        elif declare_begin == "begin":
            self.declare_block_started = False
            self.in_declare_block = False
        else:
            self.in_declare_block = True

        if self.in_declare_block and self.declare_block_started and not line.strip().upper().startswith("V_"):
            self.check_variable = True
            self.check_variable_list[f"❗ L{line_number+1}"] = line.strip()
            
    def f_check_on_comments(self, line_number, line):
        if "comment on" in line.strip().lower() and line.strip().lower().endswith("''''';"):
            self.check_empty_comment = True
            self.check_empty_comment_list[f"❗ L{line_number+1}"] = line.strip()
            
    def f_check_naming_convention(self, line_number, line):
        namin_convention = line.strip().lower()
        patterns = [
            r"create\s(or\sreplace\s)?package",
            r"create\s(or\sreplace\s)?function",
            r"create\s(or\sreplace\s)?procedure",
            r"create\s(or\sreplace\s)?view\s",
        ]
        
        patterns_match = [
            r"create\s(or\sreplace\s)?package\spck_",
            r"create\s(or\sreplace\s)?package(\sbody)?\spck_",
            r"create\s(or\sreplace\s)?function\sf_",
            r"create\s(or\sreplace\s)?procedure\sp_",
            r"create\s(or\sreplace\s)?view\sv_",
        ]
        
        for i, pattern in enumerate(patterns):
            match = re.match(pattern, namin_convention, re.IGNORECASE)
            if match:
                match2 = re.match(patterns_match[i], namin_convention, re.IGNORECASE)
                if not match2:
                    self.check_naming_convention = True
                    self.check_naming_convention_list[f"❗ L{line_number+1}"] = line.strip()
                break
                
    def f_check_parameter_convention(self, line_number, line):
        namin_convention = line.strip().lower()
        
        pattern = r'^(create\s)?(or\sreplace\s)?(procedure|function)\s.*\((.*)\)'

        # Find all matches in the input text
        match = re.match(pattern, namin_convention)
        parameters = []
        if match:
            # Extract and split the contents of group 3 by commas
            parameters = match.group(4).split(',')
            parameters = [param.strip() for param in parameters]
            
            if not all(item.lower().startswith("p_") for item in parameters):
                self.check_parameter_convention = True 
                self.check_parameter_convention_list[f"❗ L{line_number+1}"] = line.strip()
                
    def f_check_sql_keywords(self, line_number, line):
        pattern = r"'(.*?)'"
        # Use re.sub to replace the matched pattern with an empty string
        postreat = line.strip()
        postreat = re.sub(pattern, "''", postreat)
        
        for keyword in sql_keywords:
            postreat = postreat.replace(keyword.upper(), "")
        
        postreat = postreat.upper()
        postreat = postreat.split()
        postreat = [re.sub(r'[^A-Za-z0-9]', '', item) for item in postreat]
        # Convert arrays to sets for efficient intersection
        set1 = set(sql_keywords)
        set2 = set(postreat)

        # Find the common words
        common_words = set1.intersection(set2)
        if common_words:
            self.check_sql_keywords = True
            self.check_sql_keywords_list[f"❗ L{line_number+1}"] = line.strip()
            
    def f_check_on_bulk_for_in(self, line_number, line):
        postreat = line.strip().upper()
        postreat = postreat.split()
        postreat = [re.sub(r'[^A-Za-z0-9]', '', item) for item in postreat]
        # Convert arrays to sets for efficient intersection
        set1 = set(["FOR", "IN", "SELECT"])
        set2 = set(postreat)
        # Find the common words
        common_words = set1.intersection(set2)
        if len(common_words) > 2:
            self.check_on_bulk_for_in = True
            self.check_on_bulk_for_in_list[f"❗ L{line_number+1}"] = line.strip()
            
    def f_check_on_nvl(self, line_number, line):
        postreat = line.strip().upper()
        if "NVL" in postreat:
            self.check_on_nvl = True
            self.check_on_nvl_list[f"❗ L{line_number+1}"] = line.strip()
            
    def f_check_on_using(self, line_number, line):
        postreat = line.strip().upper()
        if "USING" in postreat:
            self.check_on_using = True
            self.check_on_using_list[f"❗ L{line_number+1}"] = line.strip()
            
    def loop_over_lines(self):
        
        for line_number, line in enumerate(self.lines):
            if line.strip().startswith("--")  or not line.strip():
                continue  # Skip lines starting with "--"
            self.f_check_tabulation(line_number, line)
            self.f_check_exist(line_number, line)
            self.f_check_declare_begin(line_number, line)
            self.f_check_on_comments(line_number, line)
            self.f_check_naming_convention(line_number, line)
            self.f_check_parameter_convention(line_number, line)
            self.f_check_sql_keywords(line_number, line)
            self.f_check_on_bulk_for_in(line_number, line)
            self.f_check_on_nvl(line_number, line)
    
    def show_messages(self):
        
        linter_data  = (
            ["Tabulation instead of 4 spaces", self.tabulation_check, self.tabulation_check_list],
            ["SQL Keywords must be in uppercase", self.check_sql_keywords, self.check_sql_keywords_list],
            ["The usage of exists() is forbidden", self.exist_check, self.exist_check_list],
            ["VARCHAR(digits BYTE) , BYTE is forbidden", self.byte_check, self.byte_check_list],
            ["Variables in declare must start with V_", self.check_variable, self.check_variable_list],
            ["Naming convention is not respected", self.check_naming_convention, self.check_naming_convention_list],
            ["Function & Procedures parameter naming convention", self.check_parameter_convention, self.check_parameter_convention_list],
            ["Comments must not be empty (comment on)", self.check_empty_comment, self.check_empty_comment_list],
            ["USE BULK COLLECT instead of FORALL IN SELECT", self.check_on_bulk_for_in, self.check_on_bulk_for_in_list],
            ["USE COALESCE instead of NVL", self.check_on_nvl, self.check_on_nvl_list],
            ["USE INNER JOIN INSTEAD OF USING", self.check_on_using, self.check_on_using_list],
        )
        
        for check_number, check in enumerate(linter_data):
            self.append_message(
                str(check_number+5),
                check[0],
                check[1],
                lines_data=check[2],
            )
