from setuptools import setup, find_packages

setup(
    name="motor-ingesta",
    version="0.1.0",
    author="Carlos Arturo Silva Oviedo",
    author_email="@ucm.es",
    description="Motor de ingesta para el curso de diseño de ingestas y lagos de datos",
    long_description="Este motor de ingesta se crea para realizar el curso de ingestas y lagos de datos",
    long_description_content_type="text/markdown",
    url="https://github.com/nombrealumno",
    python_requires="=3.11",
    packages=find_packages(),
    package_data={"motor_ingesta": ["resources/*.csv"]}
)