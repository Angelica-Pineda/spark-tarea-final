from setuptools import setup, find_packages

setup(
    name="motor-ingesta",
    version="0.1.0",
    author="Angelica Pineda",
    author_email="jepineda@ucm.es",
    description="Motor de ingesta para el curso de Spark",
    long_description="Motor de ingesta para el curso de Spark",
    long_description_content_type="text/markdown",
    url="https://github.com/Angelica-Pineda",
    python_requires=">=3.8",
    packages=find_packages(),
    package_data={"motor_ingesta": ["resources/*.csv"]},
    include_package_data=True,
    install_requires=[
        "loguru==0.7.1",
        "pandas>=2.0.0",
        "numpy<2.0.0" # Añadido para la compatibilidad en Databricks
    ]
)

