import csv
import os
import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox, ttk

from sql_linter import OwiLanceSqlLinter


def show_report_generated_alert():
    messagebox.showinfo("Alert", "Report generated")


def save_in_csv(
    file_name_without_extension,
    csv_data,
    folder_path=None,
):
    print("Creating CSV file...")
    print(file_name_without_extension)
    if folder_path is None:
        folder_path = os.path.join(os.getcwd(), "report")

    csv_file = os.path.join(folder_path, f"{file_name_without_extension}_report.csv")
    # Write the CSV file with header rows
    # Check if the CSV file already exists
    if os.path.exists(csv_file):
        # If it exists, open it in write mode to empty it
        with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
            file.truncate(0)  # Empty the file
    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # Write the header rows
        writer.writerow(
            ["check", "check_title", "status", "number_of_errors", "message"]
        )
        # Write the data rows
        writer.writerows(csv_data)
    show_report_generated_alert()


def check_reentrance_status(file_path, folder_path=None):
    linter_instance = OwiLanceSqlLinter()

    # linter_instance.check_file_with_chardet(file_path)

    linter_instance.check_if_file_is_ansi(file_path)

    linter_instance.open_files_and_get_lines(file_path)

    linter_instance.u_check_rentrance()

    linter_instance.u_check_fin_du_script()

    linter_instance.u_check_show_error()

    linter_instance.u_check_end()

    linter_instance.loop_over_lines()

    linter_instance.show_messages()

    save_in_csv(
        file_name_without_extension=os.path.splitext(os.path.basename(file_path))[0],
        csv_data=linter_instance.csv_data,
        folder_path=folder_path,
    )

    return linter_instance


def generate_reports_for_a_dir():
    # Ask the user to select a folder
    folder_path = fd.askdirectory()
    print(folder_path)
    sql_files = None
    if folder_path:
        # Get a list of all files in the folder with the ".sql" extension
        folder_path = os.path.join(folder_path, "report")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        print(folder_path)
        sql_files = [
            os.path.join(folder_path, file)
            for file in os.listdir(folder_path)
            if file.endswith(".sql")
        ]
    if sql_files:
        for sql_file in sql_files:
            check_reentrance_status(sql_file, folder_path)


def update_canvas_size(event):
    # canvas.config(scrollregion=canvas.bbox("all"))
    pass


def generate_report_for_a_file():
    filetypes = (("text files", "*.sql"),)
    file_path = fd.askopenfilename(filetypes=filetypes)
    if file_path:
        check_reentrance_status(file_path)
        # status_label.config(text="\n\n\n".join(linter_instance.all_messages))
        # status_label_raw.config(text="\n\n\n".join(linter_instance.raw_messages))


# Create a tkinter window
window = tk.Tk()
window.title("SQL Linter")
window.configure(bg="#5F6062")
# Set the window size to 1240x720
window.geometry("170x100")

# Add a button to trigger the status check
check_button = ttk.Button(
    window,
    text="Check Status on file",
    command=lambda: generate_report_for_a_file(),
)
check_button.grid(column=0, row=0, pady=10, padx=10)

check_on_dir = ttk.Button(
    window,
    text="Check Status on directory",
    command=lambda: generate_reports_for_a_dir(),
)
check_on_dir.grid(column=0, row=1, pady=10, padx=10)

# Update the scrollable region initially
# frame_content.update_idletasks()
# canvas.config(scrollregion=canvas.bbox("all"))

# Start the tkinter main loop
window.mainloop()
