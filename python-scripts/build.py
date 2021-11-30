from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")

default_task = "publish"
name = "dataproducts"
version = "3.7.0"
license = "MIT License"

@init
def initialize(project):
    project.depends_on_requirements("requirements.txt")
