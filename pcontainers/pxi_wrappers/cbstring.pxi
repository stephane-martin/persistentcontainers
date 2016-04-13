cdef extern from "bstrlib/bstrlib.h" nogil:
    # noinspection PyPep8Naming
    cppclass tagbstring:
        int mlen
        int slen
        unsigned char* data

cdef extern from "bstrlib/bstrwrap.h" namespace "Bstrlib" nogil:
    # noinspection PyPep8Naming
    cppclass CBString(tagbstring):
        CBString()
        CBString(char c)
        CBString(unsigned char c)
        CBString(const CBString& b)
        CBString(const tagbstring& x)
        CBString(char c, int l)
        CBString(const void * blk, int l)

        int length ()

        const CBString& operator = (char c)
        const CBString& operator = (unsigned char c)
        const CBString& operator = (const char *s)
        const CBString& operator = (const CBString& b)
        const CBString& operator = (const tagbstring& x)
        const CBString operator + (char c)
        const CBString operator + (unsigned char c)
        const CBString operator + (const unsigned char *s)
        const CBString operator + (const char *s)
        const CBString operator + (const CBString& b)
        const CBString operator + (const tagbstring& x)
        const CBString operator * (int count)
        cpp_bool operator == (const CBString& b)
        cpp_bool operator == (const char * s)
        cpp_bool operator == (const unsigned char * s)
        cpp_bool operator != (const CBString& b)
        cpp_bool operator != (const char * s)
        cpp_bool operator != (const unsigned char * s)
        cpp_bool operator <  (const CBString& b)
        cpp_bool operator <  (const char * s)
        cpp_bool operator <  (const unsigned char * s)
        cpp_bool operator <= (const CBString& b)
        cpp_bool operator <= (const char * s)
        cpp_bool operator <= (const unsigned char * s)
        cpp_bool operator >  (const CBString& b)
        cpp_bool operator >  (const char * s)
        cpp_bool operator >  (const unsigned char * s)
        cpp_bool operator >= (const CBString& b)
        cpp_bool operator >= (const char * s)
        cpp_bool operator >= (const unsigned char * s)

