from setuptools import setup, find_packages
setup(
    name="blackroad-project-management",
    version="1.0.0",
    description="BlackRoad Foundation – Project Management Engine",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[],
    extras_require={"dev": ["pytest>=7"]},
)
