import os
from pathlib import Path
from sys import platform

import jpype

REPO_DIR = Path(__file__).parent.parent
isthmus_jars = Path.joinpath(REPO_DIR, "produce_substrait/jars/*")

the_java_home = "CONDA_PREFIX"
if "JAVA_HOME" in os.environ:
    the_java_home = "JAVA_HOME"

java_home_path = os.environ[the_java_home]
jvm_path = java_home_path

if not os.path.isfile(jvm_path):
    if platform == "darwin":
        jvm_path = java_home_path + "/lib/libjli.dylib"
    elif platform == "linux":
        jvm_path = java_home_path + "/lib/server/libjvm.so"

jpype.startJVM("--enable-preview", convertStrings=True, jvmpath=jvm_path)
jpype.addClassPath(isthmus_jars)

ArrayListClass = jpype.JClass("java.util.ArrayList")
ListClass = jpype.JClass("java.util.List")
SqlToSubstraitClass = jpype.JClass("io.substrait.isthmus.SqlToSubstrait")
