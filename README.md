# go-sophia
go-sophia is a Go (golang) binding to the Sophia key-value database (http://sophia.systems/)

#Installation
First of all you need to install Sophia. Download it from http://sophia.systems/, and build.
The makefiles don't include an 'install', so you will need to manually install somewhere where Go can find the headers and the libs.
	
		export CGO_CFLAGS="-I/path/to/sophia"
		export CGO_LDFLAGS="-I/path/to/sophia"
		
Or

	CGO_CFLAGS="-I/path/to/sophia" \
    CGO_LDFLAGS="-L/path/to/sophia" \
    	go get github.com/pnevezhin/go-sophia`
    	
And then build your project with flag

	--ldflags '-extldflags "-static"'
