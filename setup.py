import setuptools

with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(

     name='pyawake',  

     version='0.1',

     scripts=['dokr'] ,

     author="Aman Singh Thakur",

     author_email="singh96aman@gmail.com",

     description="A pyawake library",

     long_description=long_description,

   long_description_content_type="text/markdown",

     url="https://github.com/singh96aman/pyawake",

     packages=setuptools.find_packages(),

     classifiers=[

         "Programming Language :: Python :: 3",

         "License :: OSI Approved :: MIT License",

         "Operating System :: OS Independent",

     ],

 )