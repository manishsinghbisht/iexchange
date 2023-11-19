# python setup.py build
# python setup.py install
# python setup.py sdist
# python setup.py bdist --formats=wininst

from setuptools import setup, find_packages

setup(name='iexchange',
      version='1',
      description='Data transfer utility',
      long_description='Data transfer utility works in close connection with main web application.',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7 (64-bit)',
        'Topic :: Data Processing :: Provider Data',
      ],
      keywords='Provider Data Exchange',
      url='https://github.com/manishsinghbisht/',
      author='Manish Singh Bisht',
      author_email='msb.net.in@gmail.com',
      license='MIT',
      packages=find_packages(),
      #packages=['root', 'root.ruleModules', 'root.apiCalls', 'root.fileModules', 'root.dbModules'],
      entry_points={
        'console_scripts': [
            #'my_start=folder_name.file_name:function_name',
            #'my_start=my_package.start:main',
            # 'my_start=my_package.__main__:main',
            'iexchange=root.__main__:main',
        ]},
      
      install_requires=[
            "jsonmerge",
            "jsonschema",
            "logzero",
            "matplotlib",
            "numpy",
            "pandas",
            "pip",
            "pymongo",
            "pyodbc",
            "python-dateutil",
            "requests",
            "setuptools",
            "urllib3",
            "uuid",
            "cx_freeze"
      ],
      include_package_data=True,
      zip_safe=False)
