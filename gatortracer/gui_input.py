import json_fetch
import tkinter
from tkinter import messagebox
from tkinter.simpledialog import askstring

token = json_fetch.Token()

def reset_token():
    name = askstring("Save token", "What's the new token")
    token.set_token(name)
    return

def exist_token():
    if token.get_token():
        messagebox.showinfo("token inf", "Token exists already")
        return
    messagebox.showinfo("token inf", "No saved token")
    return


def rm_token():
    token.remove_token()
    messagebox.showinfo("token inf", "Token has been removed")
    return


window = tkinter.Tk()
window.geometry("700x300")

def main_page():
    frame_main = tkinter.Frame(window,width=700,height=300)
    frame_main.place(x=0,y=0)
    token_page_btn = tkinter.Button(frame_main, text="use saved token",command=token_page)
    token_page_btn.pack()
    tmp_page_btn = tkinter.Button(frame_main,text="use temporary token")
    tmp_page_btn.pack()

    
def token_page():
    frame_token = tkinter.Frame(window,width=700,height=300)
    frame_token.place(x=0,y=0)
    label = tkinter.Label(
        text="Hello, Tkinter",
        foreground="white",  # Set the text color to white
        background="black",  # Set the background color to black
    )

    label.pack()

    reset_btn = tkinter.Button(frame_token,text="reset", command=reset_token)
    reset_btn.pack()
    exist_btn = tkinter.Button(frame_token,text="exist", command=exist_token)
    exist_btn.pack()
    rm_btn = tkinter.Button(frame_token,text="remove", command=rm_token)
    rm_btn.pack()
    return_main = tkinter.Button(frame_token,text = "main_page",command = main_page)
    return_main.pack()

main_page()
window.mainloop()
