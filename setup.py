from setuptools import setup, find_packages

def parse_requirements(filename):
    with open(filename) as f:
        lines = (line.strip() for line in f)
        return [line for line in lines if line and not line.startswith("#")]

setup(
    name="Weasel Bot",
    version="0.1.0",
    author="Evan Petzoldt",
    description="WeaselBot is a Slack bot designed for F3 to get the PAX in your region more engaged",
    url="https://github.com/evanpetzoldt/weaselbot",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=parse_requirements("requirements.txt"),
)
