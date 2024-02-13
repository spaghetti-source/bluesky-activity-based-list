import hashlib
import os
from datetime import datetime, timedelta

from atproto import (
    CAR,
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)
from models import Block, initialize_table, session, User
from bluesky_client_manager import BlueskyClientManager

client_manager = BlueskyClientManager()


def get_did(at_uri: str) -> str:
    return at_uri.split("/")[2]


def hash_fn(a: str) -> bytes:
    md5 = hashlib.md5(a.encode()).digest()
    return (1 << (int.from_bytes(md5, "big") % 1024)).to_bytes(1024, "big")


def bit_or(a: bytes, b: bytes) -> bytes:
    return (int.from_bytes(a, "big") | int.from_bytes(b, "big")).to_bytes(1024, "big")


def popcount(a: bytes) -> int:
    return int.from_bytes(a, "big").bit_count()


repo = "did:plc:dqxsa5cjfrzulhalom4kuyd2"  # tmaehara.bsky.social

list_uri = {
    "followed": f"at://{repo}/app.bsky.graph.list/3klaahvob3d22",
    "replied": f"at://{repo}/app.bsky.graph.list/3klaajwru372o",
    "liked": f"at://{repo}/app.bsky.graph.list/3kla6hmpa5s2e",
    "quoted": f"at://{repo}/app.bsky.graph.list/3klaalcinmk2w",
    "reposted": f"at://{repo}/app.bsky.graph.list/3klaalnbyfd2q",
}


def insert_to_list(action, did):
    global client_manager
    client = client_manager.get()

    response = client.com.atproto.repo.create_record(
        {
            "collection": "app.bsky.graph.listitem",
            "repo": repo,
            "record": {
                "subject": did,
                "list": list_uri[action],
                "createdAt": client.get_current_time_iso(),
            },
        }
    )
    rkey = response.uri.split("/")[-1]
    return rkey


def remove_from_list(rkey):
    global client_manager
    client = client_manager.get()

    response = client.com.atproto.repo.delete_record(
        {
            "collection": "app.bsky.graph.listitem",
            "repo": repo,
            "rkey": rkey,
        }
    )
    print(response)


thresholds = {
    "liked": os.environ.get("LIKED", 100),
    "replied": os.environ.get("REPLIED", 30),
    "followed": os.environ.get("FOLLOWED", 100),
    "quoted": os.environ.get("QUOTED", 30),
    "reposted": os.environ.get("REPOSTED", 30),
}


def record_interaction(source, action, target, hours=2):
    # print(source, action, target)
    value = hash_fn(target)

    curr_value = value
    for h in reversed(range(hours)):
        timestamp = (datetime.today() + timedelta(hours=h)).replace(
            minute=0, second=0, microsecond=0
        )
        user = (
            session.query(User)
            .filter(
                User.action == action,
                User.id == source,
                User.timestamp == timestamp,
            )
            .first()
        )
        if user:
            curr_value = bit_or(user.value, value)
            user.value = curr_value
            session.merge(user)
        else:
            user = User(action=action, id=source, timestamp=timestamp, value=value)
            session.add(user)

    now = datetime.today()
    curr_count = popcount(curr_value)
    if curr_count >= thresholds[action]:
        block = (
            session.query(Block)
            .filter(
                Block.action == action,
                Block.id == source,
            )
            .first()
        )
        if block:
            block.timestamp = now
        else:
            print(
                f"{source} exceeded threshold of {action}: {curr_count} >= {thresholds[action]}"
            )
            rkey = insert_to_list(action, source)

            block = Block(
                action=action,
                id=source,
                rkey=rkey,
                timestamp=now,
            )
            session.add(block)

    release = (
        session.query(Block).filter(Block.timestamp <= now - timedelta(days=1))
    ).all()
    for block in release:
        print(
            f"Delete {block.id} from {block.action} because no activity since {block.timestamp}"
        )
        remove_from_list(block.rkey)
        session.delete(block)

    session.commit()


def on_message_handler(message) -> None:
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action != "create":
            continue
        raw_data = car.blocks.get(op.cid)
        if not raw_data:
            continue

        record = models.utils.get_or_create(raw_data, strict=False)

        if models.utils.is_record_type(record, models.AppBskyGraphFollow):
            source = commit.repo
            target = record.subject
            record_interaction(source, "followed", target)

        if models.utils.is_record_type(record, models.AppBskyFeedLike):
            source = commit.repo
            target = get_did(record.subject.uri)
            record_interaction(source, "liked", target)

        if models.utils.is_record_type(record, models.AppBskyFeedPost):
            if record.reply is not None:
                source = commit.repo
                target = get_did(record.reply.parent.uri)
                record_interaction(source, "replied", target)

            if record.embed is not None:
                if models.utils.is_record_type(record.embed, models.AppBskyEmbedRecord):
                    source = commit.repo
                    target = get_did(record.embed.record.uri)
                    record_interaction(source, "quoted", target)

        if models.utils.is_record_type(record, models.AppBskyFeedRepost):
            source = commit.repo
            target = get_did(record.subject.uri)
            record_interaction(source, "reposted", target)


if __name__ == "__main__":
    print("Firehose Activity-Based Filter")
    initialize_table()

    client = FirehoseSubscribeReposClient()
    client.start(on_message_handler)
