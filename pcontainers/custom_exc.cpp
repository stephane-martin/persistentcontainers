#include <new>
#include <typeinfo>
#include <stdexcept>
#include <ios>
#include <boost/exception/exception.hpp>
#include <Python.h>
#include "_py_exceptions.h"
#include "lmdb_exceptions.h"
#include "custom_exc.h"
#include "utils.h"

namespace quiet {

using namespace lmdb;
using namespace utils;

void custom_exception_handler() {
    // Catch a handful of different errors here and turn them into the equivalent Python errors.
    try {
        if (PyErr_Occurred()) {
            ; // let the latest Python exn pass through and ignore the current one
        }
        else {
            throw;
        }

    } catch (const lmdb_nested_error& ex) {
        const boost::exception_ptr* nested_ex = boost::get_error_info<errinfo_nested_exception>(ex);
        if (nested_ex) {
            try {
                boost::rethrow_exception(*nested_ex);
            } catch (...) {
                custom_exception_handler();
            }
        } else {
            PyErr_SetString(py_lmdb_error, "Unkwown error");
        }
    } catch (const not_initialized& exn) {
        PyErr_SetCppString(py_not_initialized, exn.w());

    } catch (const empty_key& exn) {
        PyErr_SetCppString(py_empty_key, exn.w());

    } catch (const empty_database& exn) {
        PyErr_SetCppString(py_empty_database, exn.w());

    } catch (const access_error& exn) {
        PyErr_SetCppString(py_access_error, exn.w());

    } catch (const io_error& exn) {
        PyErr_SetCppString(PyExc_IOError, exn.w());

    } catch (const system_error& exn) {
        PyErr_SetCppString(PyExc_IOError, exn.w());

    } catch (const mdb_keyexist& exn) {
        PyErr_SetCppString(py_key_exist, exn.w());

    } catch (const mdb_page_notfound& exn) {
        PyErr_SetCppString(py_page_not_found, exn.w());

    } catch (const mdb_notfound& exn) {
        PyErr_SetCppString(py_not_found, exn.w());

    } catch (const mdb_currupted& exn) {
        PyErr_SetCppString(py_corrupted, exn.w());

    } catch (const mdb_panic& exn) {
        PyErr_SetCppString(py_panic, exn.w());

    } catch (const mdb_version_mismatch& exn) {
        PyErr_SetCppString(py_version_mismatch, exn.w());

    } catch (const mdb_invalid& exn) {
        PyErr_SetCppString(py_invalid, exn.w());

    } catch (const mdb_map_full& exn) {
        PyErr_SetCppString(py_map_full, exn.w());

    } catch (const mdb_dbs_full& exn) {
        PyErr_SetCppString(py_dbs_full, exn.w());

    } catch (const mdb_readers_full& exn) {
        PyErr_SetCppString(py_readers_full, exn.w());

    } catch (const mdb_tls_full& exn) {
        PyErr_SetCppString(py_tls_full, exn.w());

    } catch (const mdb_txn_full& exn) {
        PyErr_SetCppString(py_txn_full, exn.w());

    } catch (const mdb_cursor_full& exn) {
        PyErr_SetCppString(py_cursor_full, exn.w());

    } catch (const mdb_page_full& exn) {
        PyErr_SetCppString(py_page_full, exn.w());

    } catch (const mdb_map_resized& exn) {
        PyErr_SetCppString(py_map_resized, exn.w());

    } catch (const mdb_incompatible& exn) {
        PyErr_SetCppString(py_incompatible, exn.w());

    } catch (const mdb_bad_rslot& exn) {
        PyErr_SetCppString(py_bad_rslot, exn.w());

    } catch (const mdb_bad_txn& exn) {
        PyErr_SetCppString(py_bad_txn, exn.w());

    } catch (const mdb_bad_valsize& exn) {
        PyErr_SetCppString(py_bad_val_size, exn.w());

    } catch (const mdb_bad_dbi& exn) {
        PyErr_SetCppString(py_bad_dbi, exn.w());

    } catch (const lmdb_error& exn) {
        PyErr_SetCppString(py_lmdb_error, exn.w());

    // end of specific LMDB exceptions

    } catch (const std::bad_alloc& exn) {
        PyErr_SetString(PyExc_MemoryError, exn.what());

    } catch (const std::bad_cast& exn) {
        PyErr_SetString(PyExc_TypeError, exn.what());

    } catch (const std::domain_error& exn) {
        PyErr_SetString(PyExc_ValueError, exn.what());

    } catch (const std::invalid_argument& exn) {
        PyErr_SetString(PyExc_ValueError, exn.what());

    } catch (const std::ios_base::failure& exn) {
        // Unfortunately, in standard C++ we have no way of distinguishing EOF
        // from other errors here; be careful with the exception mask
        PyErr_SetString(PyExc_IOError, exn.what());

    } catch (const std::out_of_range& exn) {
        // Change out_of_range to IndexError
        PyErr_SetString(PyExc_IndexError, exn.what());

    } catch (const std::overflow_error& exn) {
        PyErr_SetString(PyExc_OverflowError, exn.what());

    } catch (const std::range_error& exn) {
        PyErr_SetString(PyExc_ArithmeticError, exn.what());

    } catch (const std::underflow_error& exn) {
        PyErr_SetString(PyExc_ArithmeticError, exn.what());

    } catch (const std::exception& exn) {
        PyErr_SetString(PyExc_RuntimeError, exn.what());

    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
    }
}

}
