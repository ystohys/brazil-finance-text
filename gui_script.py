import pre_worker
import tkinter as tk


def run_program():
    pathway = filepath_entry.get()
    batchsize = batchsize_entry.get()
    pre_worker.pre_main_run(pathway, batchsize)


window = tk.Tk()
window.title('Text File Parser')
#window.geometry("400x300+10+10")

frame_entry = tk.Frame(master=window, width=400, height=200)
frame_entry.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)

filepath_entry = tk.Entry(master=frame_entry, width=5)
lbl_filepath = tk.Label(master=frame_entry, text="\n{File pathway}")
batchsize_entry = tk.Entry(master=frame_entry, width=5)
lbl_batchsize = tk.Label(master=frame_entry, text="\n{Batch size}")

mbutton = tk.Button(master=window, text = "Go", command = run_program)
window.mainloop()