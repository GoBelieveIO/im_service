/**
 * Copyright (c) 2014-2015, GoBelieve     
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main


const NoneRelationship = 0

type Relationship int32

func NewRelationship(is_my_friend bool, is_your_friend bool, is_in_my_blacklist bool, is_in_your_blacklist bool) Relationship {
	var r Relationship

	if is_my_friend {
		r |= 0x01
	}
	if is_your_friend {
		r |= 0x02
	}
	if is_in_my_blacklist {
		r |= 0x04
	}
	if is_in_your_blacklist {
		r |= 0x08
	}
	return r
}

func (rs Relationship) IsMyFriend() bool {
	return (rs&0x01) != 0
}

func (rs Relationship) IsYourFriend() bool {
	return (rs&0x02) != 0
}

func (rs Relationship) IsInMyBlacklist() bool {
	return (rs&0x04) != 0
}

func (rs Relationship) IsInYourBlacklist() bool {
	return (rs&0x08) != 0
}

func (rs Relationship) reverse() Relationship {
	return NewRelationship(rs.IsYourFriend(), rs.IsMyFriend(), rs.IsInYourBlacklist(), rs.IsInMyBlacklist())
}
