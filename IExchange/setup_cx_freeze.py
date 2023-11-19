# python setup_cx_freeze.py build
# python setup_cx_freeze.py bdist_msi

import sys
from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {
    "packages": ['root', 'root.ruleModules', 'root.apiCalls', 'root.fileModules', 'root.dbModules'], 
    "includes":[],
    "include_files":['README.txt', 'Manifest.in', 'requirements.txt', 'config.json', 'dataTemplate.json', 'ruleConfig.json', 'systemRuleConfig.json', 'root\\internal.txt'],
    "excludes": ['tkinter'],
    'build_exe': '..\\iexchange_one_file\\'
    }

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
if sys.platform == "win32":
    base = "Win32GUI"

setup(  name = "iexchange",
        version = "0.1",
        description = "Data transfer utility",
        author = "Manish",
        author_email = "msb.net.in@gmail.com",
        options = {"build_exe": build_exe_options},
        executables = [Executable("start.py", base=base)])