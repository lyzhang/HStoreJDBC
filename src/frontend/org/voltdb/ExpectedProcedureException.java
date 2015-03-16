/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

/**
 * This exception is for errors that occur during normal operation
 * and can be handled by the runtime safely. Other exceptions can be
 * considered unexpected exceptions and should be classified as
 * VoltDB bugs.
 *
 */
public class ExpectedProcedureException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    String msg = "no exception message";
    Throwable cause = null;

    public ExpectedProcedureException(String msg) {
        this.msg = msg;
    }

    public ExpectedProcedureException(String msg, Throwable cause) {
        this.msg = msg;
        this.cause = cause;
    }

    @Override
    public String getMessage() {
        if (cause != null)
            return msg + "(" + cause.getMessage() + ")";
        else
            return msg;
    }

    public Throwable getCause() {
        return cause;
    }
}
